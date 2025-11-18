// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sub_controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "github.com/apache/doris-operator/api/disaggregated/v1"
	"github.com/apache/doris-operator/pkg/common/utils/k8s"
	"github.com/apache/doris-operator/pkg/common/utils/metadata"
	"github.com/apache/doris-operator/pkg/common/utils/mysql"
	"github.com/apache/doris-operator/pkg/common/utils/resource"
	"github.com/apache/doris-operator/pkg/common/utils/set"
	"github.com/spf13/viper"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	//doris use LOG_DIR as the key of log path. this update from 2.1.7
	//metaservice use log_dir, when used please ignore case-sensitive
	oldLogPathKey        = "LOG_DIR"
	newLogPathKey        = "sys_log_dir"
	FEMetaPathKey        = "meta_dir"
	FELogStoreName       = "fe-log"
	FEMetaStoreName      = "fe-meta"
	BELogStoreName       = "be-log"
	BECacheStorePreName  = "be-storage"
	MSLogStoreName       = "ms-log"
	DefaultCacheRootPath = "/opt/apache-doris/be/file_cache"
	//default cache storage size: unit=B
	DefaultCacheSize               int64 = 107374182400
	FileCachePathKey                     = "file_cache_path"
	FileCacheSubConfigPathKey            = "path"
	FileCacheSubConfigTotalSizeKey       = "total_size"
)

type DisaggregatedSubController interface {
	//Sync reconcile for sub controller. bool represent the component have updated.
	Sync(ctx context.Context, obj client.Object) error
	//clear all resource about sub-component.
	ClearResources(ctx context.Context, obj client.Object) (bool, error)

	//return the controller name, beController, feController,cnController for log.
	GetControllerName() string

	//UpdateStatus update the component status on src.
	UpdateComponentStatus(obj client.Object) error
}

type DisaggregatedSubDefaultController struct {
	K8sclient      client.Client
	K8srecorder    record.EventRecorder
	ControllerName string
}

// PreparePersistentVolumeClaims manages existing PVCs for StatefulSet volumeClaimTemplates
// This method handles resizing and updating existing PVCs that were created by StatefulSet.
// It does NOT create new PVCs - StatefulSet creates them automatically when pods start.
// It does NOT handle volumes that go directly to the pod template.
func (d *DisaggregatedSubDefaultController) PreparePersistentVolumeClaims(ctx context.Context, cluster client.Object, component string, statefulSet *appv1.StatefulSet, selector map[string]string, replicas int32) bool {
	// Only proceed if StatefulSet has volumeClaimTemplates
	if len(statefulSet.Spec.VolumeClaimTemplates) == 0 {
		return true // No PVCs to manage
	}

	// List existing PVCs
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pvcList := corev1.PersistentVolumeClaimList{}
	if err := d.K8sclient.List(listCtx, &pvcList, client.InNamespace(cluster.GetNamespace()), client.MatchingLabels(selector)); err != nil {
		klog.Errorf("%s preparePersistentVolumeClaims: list PVCs failed: %v", component, err)
		d.K8srecorder.Event(cluster, string(EventWarning), string(PVCListFailed), "list "+component+" PVCs failed: "+err.Error())
		return false
	}

	// Extract volume names from StatefulSet's volumeClaimTemplates
	volumeNames := make([]string, 0, len(statefulSet.Spec.VolumeClaimTemplates))
	volumeSpecs := make(map[string]corev1.PersistentVolumeClaimSpec)

	for _, vct := range statefulSet.Spec.VolumeClaimTemplates {
		volumeNames = append(volumeNames, vct.Name)
		volumeSpecs[vct.Name] = vct.Spec
	}

	// Group PVCs by volume name from StatefulSet volumeClaimTemplates
	pvcMap := make(map[string][]corev1.PersistentVolumeClaim)
	for _, pvc := range pvcList.Items {
		for _, volumeName := range volumeNames {
			if strings.HasPrefix(pvc.Name, volumeName+"-") {
				pvcMap[volumeName] = append(pvcMap[volumeName], pvc)
				break
			}
		}
	}

	// Manage existing PVCs (resize if needed) for each volume claim template
	prepared := true
	for _, volumeName := range volumeNames {
		volumeSpec, exists := volumeSpecs[volumeName]
		if !exists {
			klog.Errorf("%s preparePersistentVolumeClaims: volume claim template %s not found in StatefulSet", component, volumeName)
			prepared = false
			continue
		}

		// Create a synthetic PersistentVolume for the patchPVCs method
		pv := v1.PersistentVolume{
			PersistentVolumeClaimSpec: volumeSpec,
		}

		if !d.patchPVCs(ctx, cluster, component, selector, pvcMap[volumeName], statefulSet.Name, volumeName, pv, replicas) {
			prepared = false
		}
	}

	return prepared
}

// patchPVCs manages existing PVCs for a specific StatefulSet volumeClaimTemplate
// This handles resizing existing PVCs only - StatefulSet creates the PVCs automatically
// Does NOT create new PVCs - that's handled by StatefulSet controller
func (d *DisaggregatedSubDefaultController) patchPVCs(ctx context.Context, cluster client.Object, component string, selector map[string]string, pvcs []corev1.PersistentVolumeClaim, stsName, volumeName string, volume v1.PersistentVolume, replicas int32) bool {
	prepared := true

	// Handle existing PVCs (resize if needed)
	for _, pvc := range pvcs {
		if pvc.Spec.Resources.Requests == nil || volume.PersistentVolumeClaimSpec.Resources.Requests == nil {
			continue
		}

		oldCapacity, oldExists := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		newCapacity, newExists := volume.PersistentVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage]

		if !oldExists || !newExists || oldCapacity.Equal(newCapacity) {
			continue
		}

		// PVC needs update
		prepared = false
		eventType := EventNormal
		reason := PVCUpdate
		message := pvc.Name + " resized successfully"

		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newCapacity
		if err := d.K8sclient.Patch(ctx, &pvc, client.Merge); err != nil {
			klog.Errorf("%s patchPVCs: failed to resize PVC %s: %v", component, pvc.Name, err)
			eventType = EventWarning
			reason = PVCUpdateFailed
			message = pvc.Name + " resize failed: " + err.Error()
		}

		d.K8srecorder.Event(cluster, string(eventType), string(reason), message)
	}

	return prepared
}


func (d *DisaggregatedSubDefaultController) GetConfigValuesFromConfigMaps(namespace string, resolveKey string, cms []v1.ConfigMap) map[string]interface{} {
	if len(cms) == 0 {
		return nil
	}

	for _, cm := range cms {
		kcm, err := k8s.GetConfigMap(context.Background(), d.K8sclient, namespace, cm.Name)
		if err != nil {
			klog.Errorf("disaggregatedFEController getConfigValuesFromConfigMaps namespace=%s, name=%s, failed, err=%s", namespace, cm.Name, err.Error())
			continue
		}

		if _, ok := kcm.Data[resolveKey]; !ok {
			continue
		}

		v := kcm.Data[resolveKey]
		return d.resolveStartConfig([]byte(v), resolveKey)
	}

	return nil
}

func (d *DisaggregatedSubDefaultController) resolveStartConfig(vb []byte, resolveKey string) map[string]interface{} {
	switch resolveKey {
	case resource.MS_RESOLVEKEY:
		os.Setenv("DORIS_HOME", resource.DEFAULT_ROOT_PATH+"/ms")
	case resource.FE_RESOLVEKEY:
		os.Setenv("DORIS_HOME", resource.DEFAULT_ROOT_PATH+"/fe")
	case resource.BE_RESOLVEKEY:
		os.Setenv("DORIS_HOME", resource.DEFAULT_ROOT_PATH+"/be")
	default:
	}

	viper.SetConfigType("properties")
	viper.ReadConfig(bytes.NewBuffer(vb))
	return viper.AllSettings()
}

// for config default values.
func (d *DisaggregatedSubDefaultController) NewDefaultService(ddc *v1.DorisDisaggregatedCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ddc.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				metav1.OwnerReference{
					APIVersion: ddc.APIVersion,
					Kind:       ddc.Kind,
					Name:       ddc.Name,
					UID:        ddc.UID,
				},
			},
		},
		Spec: corev1.ServiceSpec{
			SessionAffinity: corev1.ServiceAffinityClientIP,
		},
	}
}

func (d *DisaggregatedSubDefaultController) NewDefaultStatefulset(ddc *v1.DorisDisaggregatedCluster) *appv1.StatefulSet {
	return &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ddc.Namespace,
			OwnerReferences: []metav1.OwnerReference{resource.GetOwnerReference(ddc)},
		},
		Spec: appv1.StatefulSetSpec{
			PodManagementPolicy:  appv1.ParallelPodManagement,
			RevisionHistoryLimit: metadata.GetInt32Pointer(5),
			UpdateStrategy: appv1.StatefulSetUpdateStrategy{
				Type: appv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appv1.RollingUpdateStatefulSetStrategy{
					Partition: metadata.GetInt32Pointer(0),
				},
			},
		},
	}
}

func (d *DisaggregatedSubDefaultController) BuildDefaultConfigMapVolumesVolumeMounts(cms []v1.ConfigMap) ([]corev1.Volume, []corev1.VolumeMount) {
	var vs []corev1.Volume
	var vms []corev1.VolumeMount
	for _, cm := range cms {
		v := corev1.Volume{
			Name: cm.Name,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cm.Name,
					},
				},
			},
		}

		vs = append(vs, v)
		vm := corev1.VolumeMount{
			Name:      cm.Name,
			MountPath: cm.MountPath,
		}
		if vm.MountPath == "" {
			vm.MountPath = resource.ConfigEnvPath
		}
		vms = append(vms, vm)
	}
	return vs, vms
}

func (d *DisaggregatedSubDefaultController) ConstructDefaultAffinity(matchKey, value string, ddcAffinity *corev1.Affinity) *corev1.Affinity {
	affinity := d.newDefaultAffinity(matchKey, value)

	if ddcAffinity == nil {
		return affinity
	}

	ddcPodAntiAffinity := ddcAffinity.PodAntiAffinity
	if ddcPodAntiAffinity != nil {
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = ddcPodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, ddcPodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution...)
	}

	affinity.NodeAffinity = ddcAffinity.NodeAffinity
	affinity.PodAffinity = ddcAffinity.PodAffinity

	return affinity
}

func (d *DisaggregatedSubDefaultController) newDefaultAffinity(matchKey, value string) *corev1.Affinity {
	if matchKey == "" || value == "" {
		return nil
	}

	podAffinityTerm := corev1.WeightedPodAffinityTerm{
		Weight: 20,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: matchKey, Operator: metav1.LabelSelectorOpIn, Values: []string{value}},
				},
			},
			TopologyKey: resource.NODE_TOPOLOGYKEY,
		},
	}
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{podAffinityTerm},
		},
	}
}

// the common logic to apply service, will used by fe,be,ms.
func (d *DisaggregatedSubDefaultController) DefaultReconcileService(ctx context.Context, svc *corev1.Service) (*Event, error) {
	if err := k8s.ApplyService(ctx, d.K8sclient, svc, func(nsvc, osvc *corev1.Service) bool {
		return resource.ServiceDeepEqualWithAnnoKey(nsvc, osvc, v1.DisaggregatedSpecHashValueAnnotation)
	}); err != nil {
		klog.Errorf("disaggregatedSubDefaultController reconcileService apply service namespace=%s name=%s failed, err=%s", svc.Namespace, svc.Name, err.Error())
		return &Event{Type: EventWarning, Reason: ServiceApplyedFailed, Message: err.Error()}, err
	}

	return nil, nil
}

// generate map for mountpath:secret
func (d *DisaggregatedSubDefaultController) CheckSecretMountPath(ddc *v1.DorisDisaggregatedCluster, secrets []v1.Secret) {
	var mountsMap = make(map[string]v1.Secret)
	for _, secret := range secrets {
		path := secret.MountPath
		if s, exist := mountsMap[path]; exist {
			klog.Errorf("CheckSecretMountPath error: the mountPath %s is repeated between secret: %s and secret: %s.", path, secret.SecretName, s.SecretName)
			d.K8srecorder.Event(ddc, string(EventWarning), string(SecretPathRepeated), fmt.Sprintf("the mountPath %s is repeated between secret: %s and secret: %s.", path, secret.SecretName, s.SecretName))
		}
		mountsMap[path] = secret
	}
}

// CheckSecretExist, check the secret exist or not in specify namespace.
func (d *DisaggregatedSubDefaultController) CheckSecretExist(ctx context.Context, ddc *v1.DorisDisaggregatedCluster, secrets []v1.Secret) {
	errMessage := ""
	for _, secret := range secrets {
		var s corev1.Secret
		if getErr := d.K8sclient.Get(ctx, types.NamespacedName{Namespace: ddc.Namespace, Name: secret.SecretName}, &s); getErr != nil {
			errMessage = errMessage + fmt.Sprintf("(name: %s, namespace: %s, err: %s), ", secret.SecretName, ddc.Namespace, getErr.Error())
		}
	}
	if errMessage != "" {
		klog.Errorf("CheckSecretExist error: %s.", errMessage)
		d.K8srecorder.Event(ddc, string(EventWarning), string(SecretNotExist), fmt.Sprintf("CheckSecretExist error: %s.", errMessage))
	}
}

// RestrictConditionsEqual adds two StatefulSet,
// It is used to control the conditions for comparing.
// nst StatefulSet - a new StatefulSet
// est StatefulSet - an old StatefulSet
func (d *DisaggregatedSubDefaultController) RestrictConditionsEqual(nst *appv1.StatefulSet, est *appv1.StatefulSet) {
	//shield persistent volume update when the pvcProvider=Operator
	//in webhook should intercept the volume spec updated when use statefulset pvc.
	// TODO: updates to statefulset spec for fields other than 'replicas', 'template', 'updateStrategy', 'persistentVolumeClaimRetentionPolicy' and 'minReadySeconds' are forbidden
	nst.Spec.VolumeClaimTemplates = est.Spec.VolumeClaimTemplates
}

func (d *DisaggregatedSubDefaultController) GetManagementAdminUserAndPWD(ctx context.Context, ddc *v1.DorisDisaggregatedCluster) (string, string) {
	adminUserName := "root"
	password := ""
	if ddc.Spec.AuthSecret != "" {
		secret, _ := k8s.GetSecret(ctx, d.K8sclient, ddc.Namespace, ddc.Spec.AuthSecret)
		adminUserName, password = resource.GetDorisLoginInformation(secret)
	} else if ddc.Spec.AdminUser != nil {
		adminUserName = ddc.Spec.AdminUser.Name
		password = ddc.Spec.AdminUser.Password
	}

	return adminUserName, password

}

// add cluster specification on container spec. this is useful to add common spec on different type pods, example: kerberos volume for fe and be.
func(d *DisaggregatedSubDefaultController) AddClusterSpecForPodTemplate(componentType v1.DisaggregatedComponentType, configMap map[string]interface{}, spec *v1.DorisDisaggregatedClusterSpec, pts *corev1.PodTemplateSpec){
	var c *corev1.Container
	switch componentType {
	case v1.DisaggregatedFE:
		for	i, _ := range pts.Spec.Containers {
			if pts.Spec.Containers[i].Name == resource.DISAGGREGATED_FE_MAIN_CONTAINER_NAME {
				c = &pts.Spec.Containers[i]
				break
			}
		}
	case v1.DisaggregatedBE:
		for i, _ := range pts.Spec.Containers {
			if pts.Spec.Containers[i].Name == resource.DISAGGREGATED_BE_MAIN_CONTAINER_NAME {
				c = &pts.Spec.Containers[i]
				break
			}
		}

	default:
		klog.Errorf("DisaggregatedSubDefaultController AddClusterSpecForPodTemplate componentType %s not supported.", componentType)
		return
	}

	//add pod envs
	envs := resource.BuildKerberosEnvForDDC(spec.KerberosInfo, configMap, componentType)
	if len(envs) != 0 {
		c.Env = append(c.Env, envs...)
	}

	//add kerberos volumeMounts and volumes
	volumes, volumeMounts := resource.GetDv1KerberosVolumeAndVolumeMount(spec.KerberosInfo)
	if len(volumeMounts) != 0 {
		c.VolumeMounts = append(c.VolumeMounts, volumeMounts...)
	}
	if len(volumes) != 0 {
		pts.Spec.Volumes = append(pts.Spec.Volumes, volumes...)
	}

}

//return which generation had updated the statefulset.
func(d *DisaggregatedSubDefaultController) ReturnStatefulsetUpdatedGeneration(sts *appv1.StatefulSet, annoGenerationKey string) int64 {
	if sts == nil {
		return 0
	}

	if len(sts.Annotations) == 0 {
		return 0
	}

	g_str := sts.Annotations[annoGenerationKey]
	//if g_str is empty, g will be zero, this is our expectation, so ignore parse failed or not.
	g, _ := strconv.ParseInt(g_str, 10, 64)
	return g
}

//use statefulset.status.updateRevision and pod `controller-revision-hash` annotation to check pods updated to new revision.
//if all pods used new updateRevision return true, else return false.
func(d *DisaggregatedSubDefaultController) StatefulsetControlledPodsAllUseNewUpdateRevision(stsUpdateRevision string, pods []corev1.Pod) bool {
	if stsUpdateRevision == "" {
		return false
	}

	if len(pods) ==0 {
		return false
	}


	for _, pod := range pods {
		labels := pod.Labels
		podControlledRevision := labels[resource.POD_CONTROLLER_REVISION_HASH_KEY]
		//if use selector filter pods have one controlled by new revision of statefulset, represents the new revision is working.
		if stsUpdateRevision != podControlledRevision {
			return false
		}
	}

	return true
}

func (d *DisaggregatedSubDefaultController) BuildVolumesVolumeMountsAndPVCs(confMap map[string]interface{}, componentType v1.DisaggregatedComponentType, commonSpec *v1.CommonSpec) ([]corev1.Volume, []corev1.VolumeMount, []corev1.PersistentVolumeClaim) {
	if commonSpec.PersistentVolume == nil && len(commonSpec.PersistentVolumes) == 0 {
		vs, vms := d.getEmptyDirVolumesVolumeMounts(confMap, componentType)
		return vs, vms, nil
	}

	if commonSpec.PersistentVolume != nil {
		return d.PersistentVolumeBuildVolumesVolumeMountsAndPVCs(commonSpec, confMap, componentType)
	}

	return d.PersistentVolumeArrayBuildVolumesVolumeMountsAndPVCs(commonSpec, confMap, componentType)
}

// the old config before 25.2.1, the requiredPaths should filter log path before call this function.
func (d *DisaggregatedSubDefaultController) PersistentVolumeBuildVolumesVolumeMountsAndPVCs(commonSpec *v1.CommonSpec, confMap map[string]interface{}, componentType v1.DisaggregatedComponentType) ([]corev1.Volume, []corev1.VolumeMount, []corev1.PersistentVolumeClaim) {
	v1pv := commonSpec.PersistentVolume
	if v1pv == nil {
		return nil, nil, nil
	}

	pathName := map[string]string{} /*key=path, value=name*/
	var requiredPaths []string
	switch componentType {
	case v1.DisaggregatedMS:
		//if logNotStore anywhere is true, not build pvc.
		if !commonSpec.PersistentVolume.LogNotStore && !commonSpec.LogNotStore {
			logPath := d.getLogPath(confMap, componentType)
			pathName[logPath] = MSLogStoreName
			requiredPaths = append(requiredPaths, logPath)
		}
	case v1.DisaggregatedFE:
		if !commonSpec.PersistentVolume.LogNotStore && !commonSpec.LogNotStore {
			logPath := d.getLogPath(confMap, componentType)
			pathName[logPath] = FELogStoreName
			requiredPaths = append(requiredPaths, logPath)
		}
		metaPath := d.getFEMetaPath(confMap)
		pathName[metaPath] = FEMetaStoreName
		requiredPaths = append(requiredPaths, metaPath)
	case v1.DisaggregatedBE:
		if !commonSpec.PersistentVolume.LogNotStore && !commonSpec.LogNotStore {
			logPath := d.getLogPath(confMap, componentType)
			pathName[logPath] = BELogStoreName
			requiredPaths = append(requiredPaths, logPath)
		}
		cachePaths, _ := d.getCacheMaxSizeAndPaths(confMap)
		for i, _ := range cachePaths {
			path_i := BECacheStorePreName + strconv.Itoa(i)
			pathName[cachePaths[i]] = path_i
			requiredPaths = append(requiredPaths, cachePaths[i])
		}

		//generate the last path's name, the ordinal is length of cache paths.
		baseIndex := len(cachePaths)
		for _, path := range v1pv.MountPaths {
			if _, ok := pathName[path]; ok {
				//compatible before name= storage+i
				continue
			}

			requiredPaths = append(requiredPaths, path)
			pathName[path] = BECacheStorePreName + strconv.Itoa(baseIndex)
			baseIndex = baseIndex + 1
		}

	default:

	}


	var vs []corev1.Volume
	var vms []corev1.VolumeMount
	var pvcs []corev1.PersistentVolumeClaim

	for _, path := range requiredPaths {
		name := pathName[path]
		vs = append(vs, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: name,
			}}})
		vms = append(vms, corev1.VolumeMount{Name: name, MountPath: path})
		pvcs = append(pvcs, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Annotations: v1pv.Annotations,
			},
			Spec: *v1pv.PersistentVolumeClaimSpec.DeepCopy(),
		})
	}

	return vs, vms, pvcs
}

// use array of PersistentVolume, the new config from 25.2.x
func (d *DisaggregatedSubDefaultController) PersistentVolumeArrayBuildVolumesVolumeMountsAndPVCs(commonSpec *v1.CommonSpec, confMap map[string]interface{}, componentType v1.DisaggregatedComponentType) ([]corev1.Volume, []corev1.VolumeMount, []corev1.PersistentVolumeClaim) {
	var requiredPaths []string

	//find storage mountPaths.
	switch componentType {
	case v1.DisaggregatedFE:
		metaPath := d.getFEMetaPath(confMap)
		requiredPaths = append(requiredPaths, metaPath)
	case v1.DisaggregatedBE:
		cachePaths, _ := d.getCacheMaxSizeAndPaths(confMap)
		requiredPaths = append(requiredPaths, cachePaths...)
	default:
	}

	//check logNotStore, if true should not generate log pvc.
	logNotStore := false
	for _, v1pv := range commonSpec.PersistentVolumes {
		if len(v1pv.MountPaths) != 0 {
			requiredPaths = append(requiredPaths, v1pv.MountPaths...)
		}

		logNotStore = logNotStore || v1pv.LogNotStore
	}

	//the last check logNotStore, fist check config in any one of persistentVolumes.
	if !logNotStore && !commonSpec.LogNotStore {
		logPath := d.getLogPath(confMap, componentType)
		requiredPaths = append(requiredPaths, logPath)
	}

	//generate name of persistentVolumeClaim use the mountPath
	namePath := map[string]string{}
	pathName := map[string]string{}
	for _, path := range requiredPaths {
		//use unix path separator.
		sp := strings.Split(path, "/")
		name := ""
		for i := 1; i <= len(sp); i++ {
			if sp[len(sp)-i] == "" {
				continue
			}

			if name == "" {
				name = sp[len(sp)-i]
			} else {
				name = sp[len(sp)-i] + "-" + name
			}

			if _, ok := namePath[name]; !ok {
				break
			}
		}

		namePath[name] = path
		pathName[path] = name
	}

	pathPV := map[string]*v1.PersistentVolume{}
	//the template index.
	ti := -1
	for i, v1pv := range commonSpec.PersistentVolumes {
		if len(v1pv.MountPaths) == 0 {
			ti = i
			continue
		}
		for _, mp := range v1pv.MountPaths {
			pathPV[mp] = &commonSpec.PersistentVolumes[i]
		}
	}

	var vs []corev1.Volume
	var vms []corev1.VolumeMount
	var pvcs []corev1.PersistentVolumeClaim

	//generate pvc from the last path in requiredPaths, the mountPath that  configured by user is the highest wight, so first use the v1pv to generate pvc not template v1pv.
	ss := set.NewSetString()

	for i:= len(requiredPaths); i > 0; i-- {
		path := requiredPaths[i -1]
		//if the path have build volume, vm, pvc, skip it.
		if ss.Find(path) {
			continue
		}
		ss.Add(path)

		pv, ok := pathPV[path]
		name := pathName[path]
		metadataName := strings.ReplaceAll(name, "_", "-")

		// Process volumes based on user configuration
		if ok {
			// VolumeSpec takes precedence - routes to pod template volumes (not StatefulSet)
			if pv.VolumeSpec != nil {
				// Direct pod template volume: emptyDir, configMap, secret, hostPath, etc.
				// These volumes are created directly in the pod template, not as StatefulSet volumeClaimTemplates
				vs = append(vs, corev1.Volume{Name: metadataName, VolumeSource: *pv.VolumeSpec})
				vms = append(vms, corev1.VolumeMount{Name: metadataName, MountPath: path})
				// No PVC creation needed for pod template volumes
			} else {
				// PersistentVolumeClaimSpec - routes to StatefulSet volumeClaimTemplates
				// Creates a PVC that will be managed by StatefulSet's volumeClaimTemplates
				vs = append(vs, corev1.Volume{Name: metadataName, VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: metadataName,
					}}})
				vms = append(vms, corev1.VolumeMount{Name: metadataName, MountPath: path})
				// This PVC will be added to StatefulSet.Spec.VolumeClaimTemplates
				pvcs = append(pvcs, corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:        metadataName,
						Annotations: pv.Annotations,
					},
					Spec: *pv.PersistentVolumeClaimSpec.DeepCopy(),
				})
			}
		}

		// Template volume processing (for volumes without explicit mountPaths)
		if !ok && ti != -1 {
			templatePV := commonSpec.PersistentVolumes[ti]
			// Template volumes can also specify VolumeSpec for pod template volumes
			if templatePV.VolumeSpec != nil {
				// Template with VolumeSpec - creates pod template volume
				vs = append(vs, corev1.Volume{Name: metadataName, VolumeSource: *templatePV.VolumeSpec})
				vms = append(vms, corev1.VolumeMount{Name: metadataName, MountPath: path})
				// Template volumes don't create PVCs
			} else {
				// Template with PersistentVolumeClaimSpec - creates StatefulSet volumeClaimTemplate
				vs = append(vs, corev1.Volume{Name: metadataName, VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: metadataName,
					}}})
				vms = append(vms, corev1.VolumeMount{Name: metadataName, MountPath: path})
				// Template PVC becomes StatefulSet volumeClaimTemplate
				pvcs = append(pvcs, corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:        metadataName,
						Annotations: templatePV.Annotations,
					},
					Spec: *templatePV.PersistentVolumeClaimSpec.DeepCopy(),
				})
			}
		}
	}

	return vs, vms, pvcs
}

func (d *DisaggregatedSubDefaultController) getEmptyDirVolumesVolumeMounts(confMap map[string]interface{}, componentType v1.DisaggregatedComponentType) ([]corev1.Volume, []corev1.VolumeMount) {
	switch componentType {
	case v1.DisaggregatedMS:
		return d.getMSEmptyDirVolumesVolumeMounts(confMap)
	case v1.DisaggregatedFE:
		return d.getFEEmptyDirVolumesVolumeMounts(confMap)
	case v1.DisaggregatedBE:
		return d.getBEEmptyDirVolumesVolumeMounts(confMap)
	default:
		return nil, nil
	}
}

// this function is a compensation, because the DownwardAPI annotations and labels are not mount in pod, so this function amends。
func(d *DisaggregatedSubDefaultController) AddDownwardAPI(st *appv1.StatefulSet) {
	t := &st.Spec.Template
	for index, _ := range t.Spec.Containers {
		if t.Spec.Containers[index].Name == resource.DISAGGREGATED_FE_MAIN_CONTAINER_NAME || t.Spec.Containers[index].Name == resource.DISAGGREGATED_BE_MAIN_CONTAINER_NAME ||
			t.Spec.Containers[index].Name == resource.DISAGGREGATED_MS_MAIN_CONTAINER_NAME {
			_, d_v_m := resource.GetPodInfoVolumesVolumeMounts()
			t.Spec.Containers[index].VolumeMounts = append(t.Spec.Containers[index].VolumeMounts, d_v_m...)
			break
		}
	}
}

func (d *DisaggregatedSubDefaultController) getBEEmptyDirVolumesVolumeMounts(confMap map[string]interface{}) ([]corev1.Volume, []corev1.VolumeMount) {
	vs := []corev1.Volume{
		{
			Name: BELogStoreName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	vms := []corev1.VolumeMount{
		{
			Name:      BELogStoreName,
			MountPath: d.getLogPath(confMap, v1.DisaggregatedBE),
		},
	}

	cachePaths, _ := d.getCacheMaxSizeAndPaths(confMap)
	for i, path := range cachePaths {
		vs = append(vs, corev1.Volume{
			Name: BECacheStorePreName + strconv.Itoa(i),
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
		vms = append(vms, corev1.VolumeMount{
			Name:      BECacheStorePreName + strconv.Itoa(i),
			MountPath: path,
		})
	}

	return vs, vms
}

func (d *DisaggregatedSubDefaultController) getCacheMaxSizeAndPaths(cvs map[string]interface{}) ([]string, int64) {
	v := cvs[FileCachePathKey]
	if v == nil {
		return []string{DefaultCacheRootPath}, DefaultCacheSize
	}

	var paths []string
	var maxCacheSize int64
	vbys := v.(string)
	var pa []map[string]interface{}
	err := json.Unmarshal([]byte(vbys), &pa)
	if err != nil {
		klog.Errorf("disaggregatedComputeGroupsController getStorageMaxSizeAndPaths json unmarshal file_cache_path failed, err=%s", err.Error())
		return []string{}, 0
	}

	for i, mp := range pa {
		pv := mp[FileCacheSubConfigPathKey]
		pv_str, ok := pv.(string)
		if !ok {
			klog.Errorf("disaggregatedComputeGroupsController getStorageMaxSizeAndPaths index %d have not path config.", i)
			continue
		}
		paths = append(paths, pv_str)
		cache_v := mp[FileCacheSubConfigTotalSizeKey]
		fc_size, ok := cache_v.(float64)
		cache_size := int64(fc_size)
		if !ok {
			klog.Errorf("disaggregatedComputeGroupsController getStorageMaxSizeAndPaths index %d total_size is not number.", i)
			continue
		}
		if maxCacheSize < cache_size {
			maxCacheSize = cache_size
		}
	}
	return paths, maxCacheSize
}

// use emptyDir mode generate metaservice use volume and volumeMount.
func (d *DisaggregatedSubDefaultController) getMSEmptyDirVolumesVolumeMounts(confMap map[string]interface{}) ([]corev1.Volume, []corev1.VolumeMount) {
	vs := []corev1.Volume{
		{
			Name: MSLogStoreName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	vms := []corev1.VolumeMount{
		{
			Name:      MSLogStoreName,
			MountPath: d.getLogPath(confMap, v1.DisaggregatedMS),
		},
	}
	return vs, vms
}

// use emptyDir mode generate fe use volume and volumeMount.
func (d *DisaggregatedSubDefaultController) getFEEmptyDirVolumesVolumeMounts(confMap map[string]interface{}) ([]corev1.Volume, []corev1.VolumeMount) {
	vs := []corev1.Volume{
		{
			Name: FELogStoreName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		{
			Name: FEMetaStoreName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
	vms := []corev1.VolumeMount{
		{
			Name:      FELogStoreName,
			MountPath: d.getLogPath(confMap, v1.DisaggregatedFE),
		},
		{
			Name:      FEMetaStoreName,
			MountPath: d.getFEMetaPath(confMap),
		},
	}
	return vs, vms
}

// confMap, convert use viper, the viper will convert key to lowercase.
func (d *DisaggregatedSubDefaultController) getLogPath(confMap map[string]interface{}, componentType v1.DisaggregatedComponentType) string {
	v := confMap[oldLogPathKey]
	if v != nil {
		return v.(string)
	}
	v = confMap[newLogPathKey]
	if v != nil {
		return v.(string)
	}

	//return default log path.
	switch componentType {
	case v1.DisaggregatedMS:
		return resource.DEFAULT_ROOT_PATH + "/ms/log"
	case v1.DisaggregatedFE:
		return resource.DEFAULT_ROOT_PATH + "/fe/log"
	case v1.DisaggregatedBE:
		return resource.DEFAULT_ROOT_PATH + "/be/log"
	default:
		return ""
	}
}

func (d *DisaggregatedSubDefaultController) getFEMetaPath(confMap map[string]interface{}) string {
	v := confMap[FEMetaPathKey]
	if v == nil {
		return resource.DEFAULT_ROOT_PATH + "/fe/doris-meta"
	}
	return v.(string)
}

func (d *DisaggregatedSubDefaultController) FindSecretTLSConfig(feConfMap map[string]interface{}, ddc *v1.DorisDisaggregatedCluster) (*mysql.TLSConfig, string /*secret name*/) {
	enableTLS := resource.GetString(feConfMap, resource.ENABLE_TLS_KEY)
	if enableTLS == "" {
		return nil, ""
	}

	caCertFile := resource.GetString(feConfMap, resource.TLS_CA_CERTIFICATE_PATH_KEY)
	clientCertFile := resource.GetString(feConfMap, resource.TLS_CERTIFICATE_PATH_KEY)
	clientKeyFile := resource.GetString(feConfMap, resource.TLS_PRIVATE_KEY_PATH_KEY)
	caFileName := path.Base(caCertFile)
	clientCertFileName := path.Base(clientCertFile)
	clientKeyFileName := path.Base(clientKeyFile)

	caCertDir := filepath.Dir(caCertFile)
	secretName := ""
	for _, sn := range ddc.Spec.FeSpec.Secrets {
		if sn.MountPath == caCertDir {
			secretName = sn.SecretName
			break
		}
	}

	tlsConfig := &mysql.TLSConfig{
		CAFileName:         caFileName,
		ClientCertFileName: clientCertFileName,
		ClientKeyFileName:  clientKeyFileName,
	}

	return tlsConfig, secretName
}
