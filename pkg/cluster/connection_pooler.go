package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/r3labs/diff"
	"github.com/sirupsen/logrus"
	acidzalando "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/retryutil"
)

var (
	PoolerDefaultMasterKey  = "default-master"
	PoolerDefaultReplicaKey = "default-replica"
)

// ConnectionPoolers groups connection poolers by their target (primary or replica)
// and holds the pooler auth parameters
type ConnectionPoolers struct {
	Objects        map[string]*ConnectionPoolerObjects
	LookupFunction bool
	Auth           acidv1.ConnectionPoolerAuth
}

// ConnectionPoolerObjects K8s objects that are belong to connection pooler
type ConnectionPoolerObjects struct {
	Deployment  *appsv1.Deployment
	Service     *v1.Service
	FullName    string
	Name        string
	ClusterName string
	Namespace   string
	Role        PostgresRole
	Spec        *ConnectionPoolerSpec
	// Careful with referencing cluster.spec this object pointer changes
	// during runtime and lifetime of cluster
}

type ConnectionPoolerSpec struct {
	acidv1.ConnectionPooler

	ResourceRequirements *v1.ResourceRequirements
}

func (c *Cluster) connectionPoolerFullName(role PostgresRole, poolerName string) string {
	var name string
	if poolerName != "" && poolerName != PoolerDefaultReplicaKey && poolerName != PoolerDefaultMasterKey {
		name = fmt.Sprintf("%s-%s-pooler", c.Name, poolerName)
	} else {
		name = c.Name + "-pooler"
	}
	if role == Replica && poolerName == PoolerDefaultReplicaKey {
		name = fmt.Sprintf("%s-%s", name, "repl")
	}
	return name
}

// isConnectionPoolerEnabled
func needConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needMasterConnectionPoolerWorker(spec) ||
		needReplicaConnectionPoolerWorker(spec)
}

func needMasterConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needMasterConnectionPoolerWorker(spec)
}

func needMasterConnectionPoolerWorker(spec *acidv1.PostgresSpec) bool {
	return (spec.EnableConnectionPooler != nil && *spec.EnableConnectionPooler) ||
		(spec.ConnectionPooler != nil && spec.EnableConnectionPooler == nil)
}

func needReplicaConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needReplicaConnectionPoolerWorker(spec)
}

func needReplicaConnectionPoolerWorker(spec *acidv1.PostgresSpec) bool {
	return (spec.EnableReplicaConnectionPooler != nil &&
		*spec.EnableReplicaConnectionPooler)
}

// when listing pooler k8s objects
func (c *Cluster) poolerLabelsSet(addExtraLabels bool) labels.Set {
	poolerLabels := c.labelsSet(addExtraLabels)

	// TODO should be config values
	poolerLabels["application"] = "db-connection-pooler"

	return poolerLabels
}

// Return connection pooler labels selector, which should from one point of view
// inherit most of the labels from the cluster itself, but at the same time
// have e.g. different `application` label, so that recreatePod operation will
// not interfere with it (it lists all the pods via labels, and if there would
// be no difference, it will recreate also pooler pods).
func (c *Cluster) connectionPoolerLabels(connectionPooler *ConnectionPoolerObjects, addExtraLabels bool) *metav1.LabelSelector {
	poolerLabelsSet := c.poolerLabelsSet(addExtraLabels)

	// TODO should be config values
	poolerLabelsSet["connection-pooler"] = connectionPooler.FullName

	if addExtraLabels {
		extraLabels := map[string]string{}
		extraLabels[c.OpConfig.PodRoleLabel] = string(connectionPooler.Role)

		poolerLabelsSet = labels.Merge(poolerLabelsSet, extraLabels)
	}

	if connectionPooler.Spec != nil {
		for labelk, labelv := range connectionPooler.Spec.ConnectionPoolerParameters.PodLabels {
			poolerLabelsSet[labelk] = labelv
		}
	}

	return &metav1.LabelSelector{
		MatchLabels:      poolerLabelsSet,
		MatchExpressions: nil,
	}
}

// Prepare the database for connection pooler to be used, i.e. install lookup
// function (do it first, because it should be fast and if it didn't succeed,
// it doesn't makes sense to create more K8S objects. At this moment we assume
// that necessary connection pooler user exists.
//
// After that create all the objects for connection pooler, namely a deployment
// with a chosen pooler and a service to expose it.

// have connectionpooler name in the cp object to have it immutable name
// add these cp related functions to a new cp file
// opConfig, cluster, and database name
func (c *Cluster) createConnectionPooler(LookupFunction InstallFunction) (SyncReason, error) {
	var reason SyncReason
	c.setProcessName("creating connection pooler")

	//this is essentially sync with nil as oldSpec
	if reason, err := c.syncConnectionPooler(&acidv1.Postgresql{}, &c.Postgresql, LookupFunction); err != nil {
		return reason, err
	}
	return reason, nil
}

//
// Generate pool size related environment variables.
//
// MAX_DB_CONN would specify the global maximum for connections to a target
// 	database.
//
// MAX_CLIENT_CONN is not configurable at the moment, just set it high enough.
//
// DEFAULT_SIZE is a pool size per db/user (having in mind the use case when
// 	most of the queries coming through a connection pooler are from the same
// 	user to the same db). In case if we want to spin up more connection pooler
// 	instances, take this into account and maintain the same number of
// 	connections.
//
// MIN_SIZE is a pool's minimal size, to prevent situation when sudden workload
// 	have to wait for spinning up a new connections.
//
// RESERVE_SIZE is how many additional connections to allow for a pooler.
func (c *Cluster) getConnectionPoolerEnvVars(mode string, maxDBConn int32, numberOfInstances int32) []v1.EnvVar {

	maxDBConn = maxDBConn / numberOfInstances

	defaultSize := maxDBConn / 2
	minSize := defaultSize / 2
	reserveSize := minSize

	return []v1.EnvVar{
		{
			Name:  "CONNECTION_POOLER_PORT",
			Value: fmt.Sprint(pgPort),
		},
		{
			Name:  "CONNECTION_POOLER_MODE",
			Value: mode,
		},
		{
			Name:  "CONNECTION_POOLER_DEFAULT_SIZE",
			Value: fmt.Sprint(defaultSize),
		},
		{
			Name:  "CONNECTION_POOLER_MIN_SIZE",
			Value: fmt.Sprint(minSize),
		},
		{
			Name:  "CONNECTION_POOLER_RESERVE_SIZE",
			Value: fmt.Sprint(reserveSize),
		},
		{
			Name:  "CONNECTION_POOLER_MAX_CLIENT_CONN",
			Value: fmt.Sprint(constants.ConnectionPoolerMaxClientConnections),
		},
		{
			Name:  "CONNECTION_POOLER_MAX_DB_CONN",
			Value: fmt.Sprint(maxDBConn),
		},
	}
}

func (c *Cluster) generateConnectionPoolerPodTemplate(role PostgresRole, connectionPoolerName string, connectionPoolerSpec *ConnectionPoolerSpec, podLabels map[string]string) (
	*v1.PodTemplateSpec, error) {
	spec := &c.Spec
	gracePeriod := int64(c.OpConfig.PodTerminateGracePeriod.Seconds())

	secretSelector := func(key string) *v1.SecretKeySelector {
		return &v1.SecretKeySelector{
			LocalObjectReference: v1.LocalObjectReference{
				Name: c.credentialSecretName(connectionPoolerSpec.User),
			},
			Key: key,
		}
	}

	envVars := []v1.EnvVar{
		{
			Name:  "PGHOST",
			Value: c.serviceAddress(role),
		},
		{
			Name:  "PGPORT",
			Value: fmt.Sprint(c.servicePort(role)),
		},
		{
			Name: "PGUSER",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: secretSelector("username"),
			},
		},
		// the convention is to use the same schema name as
		// connection pooler username
		{
			Name:  "PGSCHEMA",
			Value: connectionPoolerSpec.Schema,
		},
		{
			Name: "PGPASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: secretSelector("password"),
			},
		},
	}
	envVars = append(
		envVars,
		c.getConnectionPoolerEnvVars(
			connectionPoolerSpec.Mode,
			*connectionPoolerSpec.MaxDBConnections,
			*connectionPoolerSpec.NumberOfInstances,
		)...)

	poolerContainer := v1.Container{
		Name:            connectionPoolerContainer,
		Image:           connectionPoolerSpec.DockerImage,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *connectionPoolerSpec.ResourceRequirements,
		Ports: []v1.ContainerPort{
			{
				ContainerPort: pgPort,
				Protocol:      v1.ProtocolTCP,
			},
		},
		Env: envVars,
		ReadinessProbe: &v1.Probe{
			Handler: v1.Handler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.IntOrString{IntVal: pgPort},
				},
			},
		},
		SecurityContext: &v1.SecurityContext{
			AllowPrivilegeEscalation: util.False(),
		},
	}

	tolerationsSpec := tolerations(&spec.Tolerations, c.OpConfig.PodToleration)

	var specForPodAnnotation *acidv1.PostgresSpec
	var podAnnotations map[string]string

	if !connectionPoolerSpec.DisablePostgresPodAnnotations {
		specForPodAnnotation = spec
		podAnnotations = c.annotationsSet(c.generatePodAnnotations(specForPodAnnotation))
	}

	if podAnnLen := len(connectionPoolerSpec.ConnectionPoolerParameters.PodAnnotations); podAnnotations == nil && podAnnLen > 0 {
		podAnnotations = make(map[string]string, podAnnLen)
	}

	for annotationk, annotationv := range connectionPoolerSpec.ConnectionPoolerParameters.PodAnnotations {
		podAnnotations[annotationk] = annotationv
	}

	podTemplate := &v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      podLabels,
			Namespace:   c.Namespace,
			Annotations: podAnnotations,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers:                    []v1.Container{poolerContainer},
			Tolerations:                   tolerationsSpec,
		},
	}

	specNodeAffinity := connectionPoolerSpec.NodeAffinity
	if specNodeAffinity == nil {
		specNodeAffinity = spec.NodeAffinity
	}

	nodeAffinity := c.nodeAffinity(c.OpConfig.NodeReadinessLabel, specNodeAffinity)
	if c.OpConfig.EnablePodAntiAffinity {
		labelsSet := labels.Set(podLabels)
		podTemplate.Spec.Affinity = generatePodAffinity(
			labelsSet,
			c.OpConfig.PodAntiAffinityTopologyKey,
			nodeAffinity,
			c.OpConfig.PodAntiAffinityPreferredDuringScheduling,
		)
	} else if nodeAffinity != nil {
		podTemplate.Spec.Affinity = nodeAffinity
	}

	return podTemplate, nil
}

func (c *Cluster) generateConnectionPoolerDeployment(connectionPooler *ConnectionPoolerObjects, connectionPoolerSpec *ConnectionPoolerSpec) (
	*appsv1.Deployment, error) {

	podLabelSelector := c.connectionPoolerLabels(connectionPooler, true)

	// there are two ways to enable connection pooler, either to specify a
	// connectionPooler section or enableConnectionPooler. In the second case
	// spec.connectionPooler will be nil, so to make it easier to calculate
	// default values, initialize it to an empty structure. It could be done
	// anywhere, but here is the earliest common entry point between sync and
	// create code, so init here.
	podTemplate, err := c.generateConnectionPoolerPodTemplate(connectionPooler.Role, connectionPooler.FullName, connectionPoolerSpec, podLabelSelector.MatchLabels)

	if err != nil {
		return nil, err
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        connectionPooler.FullName,
			Namespace:   connectionPooler.Namespace,
			Labels:      podLabelSelector.MatchLabels,
			Annotations: c.AnnotationsToPropagate(c.annotationsSet(nil)),
			// make StatefulSet object its owner to represent the dependency.
			// By itself StatefulSet is being deleted with "Orphaned"
			// propagation policy, which means that it's deletion will not
			// clean up this deployment, but there is a hope that this object
			// will be garbage collected if something went wrong and operator
			// didn't deleted it.
			OwnerReferences: c.ownerReferences(),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: connectionPoolerSpec.NumberOfInstances,
			Selector: podLabelSelector,
			Template: *podTemplate,
		},
	}

	return deployment, nil
}

func (c *Cluster) generateConnectionPoolerService(connectionPooler *ConnectionPoolerObjects, connectionPoolerSpec *ConnectionPoolerSpec) *v1.Service {

	spec := &c.Spec
	serviceSpec := v1.ServiceSpec{
		Ports: []v1.ServicePort{
			{
				Name:       connectionPooler.FullName,
				Port:       pgPort,
				TargetPort: intstr.IntOrString{IntVal: c.servicePort(connectionPooler.Role)},
			},
		},
		Type: v1.ServiceTypeClusterIP,
		Selector: map[string]string{
			"connection-pooler": connectionPooler.FullName,
		},
	}

	if c.shouldCreateLoadBalancerForPoolerService(connectionPooler, connectionPoolerSpec, spec) {
		c.configureLoadBalanceService(&serviceSpec, spec.AllowedSourceRanges)
	}

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        connectionPooler.FullName,
			Namespace:   connectionPooler.Namespace,
			Labels:      c.connectionPoolerLabels(connectionPooler, false).MatchLabels,
			Annotations: c.annotationsSet(c.generateServiceAnnotations(connectionPooler.Role, spec, connectionPooler.Name)),
			// make StatefulSet object its owner to represent the dependency.
			// By itself StatefulSet is being deleted with "Orphaned"
			// propagation policy, which means that it's deletion will not
			// clean up this service, but there is a hope that this object will
			// be garbage collected if something went wrong and operator didn't
			// deleted it.
			OwnerReferences: c.ownerReferences(),
		},
		Spec: serviceSpec,
	}

	if connectionPoolerSpec.ServiceAnnotations != nil {
		if service.Annotations == nil {
			service.Annotations = make(map[string]string, len(connectionPoolerSpec.ServiceAnnotations))
		}
		for k, v := range connectionPoolerSpec.ServiceAnnotations {
			service.Annotations[k] = v
		}
	}

	return service
}

func (c *Cluster) shouldCreateLoadBalancerForPoolerService(connectionPooler *ConnectionPoolerObjects, connectionPoolerSpec *ConnectionPoolerSpec, spec *acidv1.PostgresSpec) bool {

	if connectionPoolerSpec.EnableLoadBalancerService != nil {
		return *connectionPoolerSpec.EnableLoadBalancerService
	}

	switch connectionPooler.Role {
	case Replica:
		// if the value is explicitly set in a Postgresql manifest, follow this setting
		if spec.EnableReplicaPoolerLoadBalancer != nil {
			return *spec.EnableReplicaPoolerLoadBalancer
		}
		// otherwise, follow the operator configuration
		return c.OpConfig.EnableReplicaPoolerLoadBalancer

	case Master:
		if spec.EnableMasterPoolerLoadBalancer != nil {
			return *spec.EnableMasterPoolerLoadBalancer
		}
		return c.OpConfig.EnableMasterPoolerLoadBalancer

	default:
		panic(fmt.Sprintf("Unknown role %v", connectionPooler.Role))
	}
}

func (c *Cluster) listPoolerPods(listOptions metav1.ListOptions) ([]v1.Pod, error) {
	pods, err := c.KubeClient.Pods(c.Namespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("could not get list of pooler pods: %v", err)
	}
	return pods.Items, nil
}

//delete connection pooler
func (c *Cluster) deleteConnectionPooler(connectionPooler *ConnectionPoolerObjects) (err error) {
	c.logger.Infof("deleting connection pooler connection-pooler=%s spilo-role=%s", connectionPooler.Name, connectionPooler.Role)

	// Lack of connection pooler objects is not a fatal error, just log it if
	// it was present before in the manifest
	if connectionPooler == nil {
		c.logger.Debugf("no connection pooler to delete")
		return nil
	}

	// Clean up the deployment object. If deployment resource we've remembered
	// is somehow empty, try to delete based on what would we generate
	deployment := connectionPooler.Deployment
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}

	if deployment != nil {

		// set delete propagation policy to foreground, so that replica set will be
		// also deleted.

		err = c.KubeClient.
			Deployments(c.Namespace).
			Delete(context.TODO(), deployment.Name, options)

		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("connection pooler deployment was already deleted")
		} else if err != nil {
			return fmt.Errorf("could not delete connection pooler deployment: %v", err)
		}

		c.logger.Infof("connection pooler deployment %s has been deleted for role %s", deployment.Name, connectionPooler.Role)
	}

	// Repeat the same for the service object
	service := connectionPooler.Service
	if service == nil {
		c.logger.Debugf("no connection pooler service object to delete")
	} else {

		err = c.KubeClient.
			Services(c.Namespace).
			Delete(context.TODO(), service.Name, options)

		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("connection pooler service was already deleted")
		} else if err != nil {
			return fmt.Errorf("could not delete connection pooler service: %v", err)
		}

		c.logger.Infof("connection pooler service %s has been deleted for role %s", service.Name, connectionPooler.Role)
	}

	connectionPooler.Deployment = nil
	connectionPooler.Service = nil
	return nil
}

//delete connection pooler
func (c *Cluster) deleteConnectionPoolerSecret() (err error) {
	// Repeat the same for the secret object
	secretName := c.credentialSecretName(c.OpConfig.ConnectionPooler.User)

	secret, err := c.KubeClient.
		Secrets(c.Namespace).
		Get(context.TODO(), secretName, metav1.GetOptions{})

	if err != nil {
		c.logger.Debugf("could not get connection pooler secret %s: %v", secretName, err)
	} else {
		if err = c.deleteSecret(secret.UID, *secret); err != nil {
			return fmt.Errorf("could not delete pooler secret: %v", err)
		}
	}
	return nil
}

// Perform actual patching of a connection pooler deployment, assuming that all
// the check were already done before.
func updateConnectionPoolerDeployment(KubeClient k8sutil.KubernetesClient, newDeployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	if newDeployment == nil {
		return nil, fmt.Errorf("there is no connection pooler in the cluster")
	}

	patchData, err := specPatch(newDeployment.Spec)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the connection pooler deployment: %v", err)
	}

	// An update probably requires RetryOnConflict, but since only one operator
	// worker at one time will try to update it chances of conflicts are
	// minimal.
	deployment, err := KubeClient.
		Deployments(newDeployment.Namespace).Patch(
		context.TODO(),
		newDeployment.Name,
		types.StrategicMergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch connection pooler deployment: %v", err)
	}

	return deployment, nil
}

//updateConnectionPoolerAnnotations updates the annotations of connection pooler deployment
func updateConnectionPoolerAnnotations(KubeClient k8sutil.KubernetesClient, deployment *appsv1.Deployment, annotations map[string]string) (*appsv1.Deployment, error) {
	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the connection pooler deployment metadata: %v", err)
	}
	result, err := KubeClient.Deployments(deployment.Namespace).Patch(
		context.TODO(),
		deployment.Name,
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch connection pooler annotations %q: %v", patchData, err)
	}
	return result, nil

}

// Test if two connection pooler configuration needs to be synced. For simplicity
// compare not the actual K8S objects, but the configuration itself and request
// sync if there is any difference.
func needSyncConnectionPoolerSpecs(oldSpec, newSpec *acidv1.ConnectionPooler, logger *logrus.Entry) (sync bool, reasons []string) {
	reasons = []string{}
	sync = false

	changelog, err := diff.Diff(oldSpec, newSpec)
	if err != nil {
		logger.Infof("cannot get diff, do not do anything, %+v", err)
		return false, reasons
	}

	if len(changelog) > 0 {
		sync = true
	}

	for _, change := range changelog {
		msg := fmt.Sprintf("%s %+v from '%+v' to '%+v'",
			change.Type, change.Path, change.From, change.To)
		reasons = append(reasons, msg)
	}

	return sync, reasons
}

// Check if we need to synchronize connection pooler deployment due to new
// defaults, that are different from what we see in the DeploymentSpec
func (c *Cluster) needSyncConnectionPoolerDefaults(Config *Config, spec *ConnectionPoolerSpec, deployment *appsv1.Deployment) (sync bool, reasons []string) {

	reasons = []string{}
	sync = false

	podTemplate := deployment.Spec.Template
	poolerContainer := podTemplate.Spec.Containers[constants.ConnectionPoolerContainer]

	if *deployment.Spec.Replicas != *spec.NumberOfInstances {
		sync = true
		msg := fmt.Sprintf("numberOfInstances is different (having %d, required %d)",
			*deployment.Spec.Replicas, *spec.NumberOfInstances)
		reasons = append(reasons, msg)
	}

	if poolerContainer.Image != spec.DockerImage {
		sync = true
		msg := fmt.Sprintf("dockerImage is different (having %s, required %s)",
			poolerContainer.Image, spec.DockerImage)
		reasons = append(reasons, msg)
	}

	// An error to generate expected resources means something is not quite
	// right, but for the purpose of robustness do not panic here, just report
	// and ignore resources comparison (in the worst case there will be no
	// updates for new resource values).
	if syncResources(&poolerContainer.Resources, spec.ResourceRequirements) {
		sync = true
		msg := fmt.Sprintf("resources are different (having %+v, required %+v)",
			poolerContainer.Resources, spec.ResourceRequirements)
		reasons = append(reasons, msg)
	}

	for _, env := range poolerContainer.Env {
		if spec.User == "" && env.Name == "PGUSER" {
			ref := env.ValueFrom.SecretKeyRef.LocalObjectReference
			secretName := Config.OpConfig.SecretNameTemplate.Format(
				"username", strings.Replace(spec.User, "_", "-", -1),
				"cluster", c.Name,
				"tprkind", acidv1.PostgresCRDResourceKind,
				"tprgroup", acidzalando.GroupName)

			if ref.Name != secretName {
				sync = true
				msg := fmt.Sprintf("pooler user and secret are different (having %s, required %s)",
					ref.Name, secretName)
				reasons = append(reasons, msg)
			}
		}

		if spec.Schema == "" && env.Name == "PGSCHEMA" && env.Value != spec.Schema {
			sync = true
			msg := fmt.Sprintf("pooler schema is different (having %s, required %s)",
				env.Value, spec.Schema)
			reasons = append(reasons, msg)
		}
	}

	return sync, reasons
}

func (c *Cluster) needSyncConnectionPoolerSpec(oldSpec *ConnectionPoolerSpec, newSpec *ConnectionPoolerSpec) (sync bool, reasons []string) {
	reasons = []string{}
	sync = false

	changelog, err := diff.Diff(oldSpec, newSpec)
	if err != nil {
		c.logger.Infof("cannot get diff, do not do anything, %+v", err)
		return false, reasons
	}

	if len(changelog) > 0 {
		sync = true
	}

	for _, change := range changelog {
		msg := fmt.Sprintf("%s %+v from '%+v' to '%+v'",
			change.Type, change.Path, change.From, change.To)
		reasons = append(reasons, msg)
	}

	return sync, reasons
}

// Generate default resource section for connection pooler deployment, to be
// used if nothing custom is specified in the manifest
func makeDefaultConnectionPoolerResources(config *config.Config) acidv1.Resources {

	defaultRequests := acidv1.ResourceDescription{
		CPU:    config.ConnectionPooler.ConnectionPoolerDefaultCPURequest,
		Memory: config.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest,
	}
	defaultLimits := acidv1.ResourceDescription{
		CPU:    config.ConnectionPooler.ConnectionPoolerDefaultCPULimit,
		Memory: config.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit,
	}

	return acidv1.Resources{
		ResourceRequests: defaultRequests,
		ResourceLimits:   defaultLimits,
	}
}

func logPoolerEssentials(log *logrus.Entry, oldSpec, newSpec *acidv1.Postgresql) {
	var v []string
	var input []*bool

	newMasterConnectionPoolerEnabled := needMasterConnectionPoolerWorker(&newSpec.Spec)
	if oldSpec == nil {
		input = []*bool{nil, nil, &newMasterConnectionPoolerEnabled, newSpec.Spec.EnableReplicaConnectionPooler}
	} else {
		oldMasterConnectionPoolerEnabled := needMasterConnectionPoolerWorker(&oldSpec.Spec)
		input = []*bool{&oldMasterConnectionPoolerEnabled, oldSpec.Spec.EnableReplicaConnectionPooler, &newMasterConnectionPoolerEnabled, newSpec.Spec.EnableReplicaConnectionPooler}
	}

	for _, b := range input {
		if b == nil {
			v = append(v, "nil")
		} else {
			v = append(v, fmt.Sprintf("%v", *b))
		}
	}

	log.Debugf("syncing connection pooler (master, replica) from (%v, %v) to (%v, %v)", v[0], v[1], v[2], v[3])
}

// buildConnectionPoolerSpec returns a connection pooler spec with the final parameters
// to be apply, merging default orchestrator, cluster and pooler parameters
func (c *Cluster) buildConnectionPoolerSpec(spec *acidv1.PostgresSpec, role PostgresRole, connectionPoolerParameters *acidv1.ConnectionPoolerParameters) (*ConnectionPoolerSpec, error) {

	var connectionPoolerSpec ConnectionPoolerSpec

	connectionPoolerDefaults := spec.ConnectionPooler
	if connectionPoolerDefaults == nil {
		connectionPoolerDefaults = &acidv1.ConnectionPooler{}
	}

	connectionPoolerSpec.ConnectionPooler = acidv1.ConnectionPooler{
		ConnectionPoolerParameters: *connectionPoolerParameters,
		ConnectionPoolerAuth:       c.ConnectionPoolers.Auth,
	}

	connectionPoolerSpec.ConnectionPooler.Mode = util.Coalesce(
		connectionPoolerParameters.Mode,
		util.Coalesce(
			connectionPoolerSpec.Mode,
			c.OpConfig.ConnectionPooler.Mode),
	)

	numberOfInstances := util.CoalesceInt32(
		util.CoalesceInt32(
			util.CoalesceInt32(
				connectionPoolerParameters.NumberOfInstances,
				connectionPoolerDefaults.NumberOfInstances,
			),
			c.OpConfig.ConnectionPooler.NumberOfInstances,
		),
		k8sutil.Int32ToPointer(1),
	)

	if *numberOfInstances < constants.ConnectionPoolerMinInstances {
		msg := "adjusted number of connection pooler instances from %d to %d"
		c.logger.Warningf(msg, *numberOfInstances, constants.ConnectionPoolerMinInstances)

		*numberOfInstances = constants.ConnectionPoolerMinInstances
	}
	connectionPoolerSpec.ConnectionPooler.NumberOfInstances = numberOfInstances

	connectionPoolerSpec.ConnectionPooler.MaxDBConnections = util.CoalesceInt32(
		util.CoalesceInt32(
			connectionPoolerParameters.MaxDBConnections,
			util.CoalesceInt32(
				connectionPoolerSpec.MaxDBConnections,
				c.OpConfig.ConnectionPooler.MaxDBConnections),
		),
		k8sutil.Int32ToPointer(constants.ConnectionPoolerMaxDBConnections),
	)

	connectionPoolerSpec.DockerImage = util.Coalesce(
		connectionPoolerParameters.DockerImage,
		c.OpConfig.ConnectionPooler.Image)

	resources, err := c.generateResourceRequirements(
		connectionPoolerParameters.Resources,
		makeDefaultConnectionPoolerResources(&c.OpConfig),
		connectionPoolerContainer)

	if err != nil {
		return nil, err
	}

	connectionPoolerSpec.ResourceRequirements = resources

	return &connectionPoolerSpec, nil
}

func (c *Cluster) syncConnectionPooler(oldSpec, newSpec *acidv1.Postgresql, LookupFunction InstallFunction) (SyncReason, error) {

	var reason SyncReason
	var err error

	logPoolerEssentials(c.logger, oldSpec, newSpec)

	// if the call is via createConnectionPooler, then it is required to initialize
	// the structure
	if c.ConnectionPoolers == nil {
		c.ConnectionPoolers = &ConnectionPoolers{
			Objects: map[string]*ConnectionPoolerObjects{},
		}
	}

	connectionPoolerDefaults := c.Spec.ConnectionPooler
	specSchema := ""
	specUser := ""

	if connectionPoolerDefaults != nil {
		specSchema = connectionPoolerDefaults.Schema
		specUser = connectionPoolerDefaults.User
	}

	c.ConnectionPoolers.Auth = acidv1.ConnectionPoolerAuth{
		Schema: util.Coalesce(
			specSchema,
			c.OpConfig.ConnectionPooler.Schema),
		User: util.Coalesce(
			specUser,
			c.OpConfig.ConnectionPooler.User),
	}

	connectionPoolers := newSpec.Spec.ConnectionPoolers

	// if we have no list of connection poolers
	// we have only the single default cluster wide connection pooler parameters
	if len(connectionPoolers) == 0 {
		connectionPoolers = map[string]*acidv1.ConnectionPoolerParameters{}
	}

	if newSpec.Spec.DisableDefaultPooler == nil || !*newSpec.Spec.DisableDefaultPooler {

		if needMasterConnectionPooler(&newSpec.Spec) {
			parameters := &acidv1.ConnectionPoolerParameters{}

			if newSpec.Spec.ConnectionPooler != nil {
				parameters = newSpec.Spec.ConnectionPooler.ConnectionPoolerParameters.DeepCopy()
			}

			parameters.Target = string(Master)
			connectionPoolers[PoolerDefaultMasterKey] = parameters
		}

		if needReplicaConnectionPooler(&newSpec.Spec) {

			parameters := &acidv1.ConnectionPoolerParameters{}

			if newSpec.Spec.ConnectionPooler != nil {
				parameters = newSpec.Spec.ConnectionPooler.ConnectionPoolerParameters.DeepCopy()
			}

			parameters.Target = string(Replica)
			connectionPoolers[PoolerDefaultReplicaKey] = parameters
		}
	}

	oldPoolerSpecs := c.ConnectionPoolerSpecs

	// build new pooler specs to compare with previous and sync state
	newPoolerSpecs := make(map[string]*ConnectionPoolerSpec, len(connectionPoolers))

	for poolerName, poolerParameters := range connectionPoolers {
		poolerParameters = poolerParameters.DeepCopy()

		if poolerParameters.Target == "" {
			poolerParameters.Target = string(Master)
		}

		// create the connection pooler objects if they don't exist yet
		role := PostgresRole(poolerParameters.Target)
		if _, ok := c.ConnectionPoolers.Objects[poolerName]; !ok {
			c.ConnectionPoolers.Objects[poolerName] = &ConnectionPoolerObjects{
				Deployment:  nil,
				Service:     nil,
				FullName:    c.connectionPoolerFullName(role, poolerName),
				Name:        poolerName,
				Spec:        &ConnectionPoolerSpec{},
				ClusterName: c.Name,
				Namespace:   c.Namespace,
				Role:        role,
			}
		}

		newPoolerSpecs[poolerName], err = c.buildConnectionPoolerSpec(
			&newSpec.Spec,
			role,
			poolerParameters,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to build connection pooler spec: %w", err)
		}
	}

	// Try to sync in any case. If we didn't needed connection pooler before,
	// it means we want to create it. If it was already present, still sync
	// since it could happen that there is no difference in specs, and all
	// the resources are remembered, but the deployment was manually deleted
	// in between

	// in this case also do not forget to install lookup function
	// skip installation in standby clusters, since they are read-only
	if !c.ConnectionPoolers.LookupFunction && c.Spec.StandbyCluster == nil {
		if err = LookupFunction(c.ConnectionPoolers.Auth.Schema, c.ConnectionPoolers.Auth.User); err != nil {
			return NoSync, err
		}
		c.ConnectionPoolers.LookupFunction = true
	}

	for poolerName, pooler := range c.ConnectionPoolers.Objects {
		// if we still have params for this pooler let's try syncing it
		if newPoolerSpec, ok := newPoolerSpecs[poolerName]; ok {
			if reason, err = c.syncConnectionPoolerWorker(oldSpec, newSpec, PostgresRole(newPoolerSpec.Target), pooler, newPoolerSpec); err != nil {
				c.logger.Errorf("could not sync connection pooler: %v", err)
				return reason, err
			}
			pooler.Spec = newPoolerSpec
		} else {
			// no params for this pooler anymore, let's delete it
			if pooler.Deployment != nil || pooler.Service != nil {
				if err = c.deleteConnectionPooler(pooler); err != nil {
					c.logger.Warningf("could not remove connection pooler: %v", err)
				} else {
					delete(c.ConnectionPoolers.Objects, poolerName)
				}
			}
		}
	}

	if len(oldPoolerSpecs) > 0 && len(newPoolerSpecs) == 0 {
		if err = c.deleteConnectionPoolerSecret(); err != nil {
			c.logger.Warningf("could not remove connection pooler secret: %v", err)
		}
	}

	c.ConnectionPoolerSpecs = newPoolerSpecs

	return reason, nil
}

// Synchronize connection pooler resources. Effectively we're interested only in
// synchronizing the corresponding deployment, but in case of deployment or
// service is missing, create it. After checking, also remember an object for
// the future references.
func (c *Cluster) syncConnectionPoolerWorker(oldSpec, newSpec *acidv1.Postgresql, role PostgresRole, connectionPooler *ConnectionPoolerObjects, newConnectionPoolerSpec *ConnectionPoolerSpec) (
	SyncReason, error) {

	c.logger.Infof("syncing pooler worker %s", connectionPooler.FullName)

	var (
		deployment    *appsv1.Deployment
		newDeployment *appsv1.Deployment
		pods          []v1.Pod
		service       *v1.Service
		newService    *v1.Service
		err           error
	)

	syncReason := make([]string, 0)
	deployment, err = c.KubeClient.
		Deployments(c.Namespace).
		Get(context.TODO(), connectionPooler.FullName, metav1.GetOptions{})

	if err != nil && k8sutil.ResourceNotFound(err) {
		c.logger.Warningf("deployment %s for connection pooler synchronization is not found, create it", connectionPooler.FullName)

		newDeployment, err = c.generateConnectionPoolerDeployment(connectionPooler, newConnectionPoolerSpec)
		if err != nil {
			return NoSync, fmt.Errorf("could not generate deployment for connection pooler: %v", err)
		}

		deployment, err = c.KubeClient.
			Deployments(newDeployment.Namespace).
			Create(context.TODO(), newDeployment, metav1.CreateOptions{})

		if err != nil {
			return NoSync, err
		}
		connectionPooler.Deployment = deployment
	} else if err != nil {
		return NoSync, fmt.Errorf("could not get connection pooler deployment to sync: %v", err)
	} else {
		connectionPooler.Deployment = deployment

		// actual synchronization
		poolerSpecSync, poolerSpecReason := c.needSyncConnectionPoolerSpec(
			connectionPooler.Spec,
			newConnectionPoolerSpec,
		)
		syncReason = append(syncReason, poolerSpecReason...)

		defaultsSync, defaultsReason := c.needSyncConnectionPoolerDefaults(&c.Config, newConnectionPoolerSpec, deployment)
		syncReason = append(syncReason, defaultsReason...)

		if poolerSpecSync || defaultsSync {
			c.logger.Infof("update connection pooler deployment %s, reason: %+v",
				connectionPooler.FullName, syncReason)
			newDeployment, err = c.generateConnectionPoolerDeployment(connectionPooler, newConnectionPoolerSpec)
			if err != nil {
				return syncReason, fmt.Errorf("could not generate deployment for connection pooler: %v", err)
			}

			deployment, err = updateConnectionPoolerDeployment(c.KubeClient, newDeployment)

			if err != nil {
				return syncReason, err
			}
			connectionPooler.Deployment = deployment
		}
	}

	newAnnotations := c.AnnotationsToPropagate(c.annotationsSet(connectionPooler.Deployment.Annotations))
	if newAnnotations != nil {
		deployment, err = updateConnectionPoolerAnnotations(c.KubeClient, connectionPooler.Deployment, newAnnotations)
		if err != nil {
			return nil, err
		}
		connectionPooler.Deployment = deployment
	}

	// check if pooler pods must be replaced due to secret update
	listOptions := metav1.ListOptions{
		LabelSelector: labels.Set(c.connectionPoolerLabels(connectionPooler, true).MatchLabels).String(),
	}
	pods, err = c.listPoolerPods(listOptions)
	if err != nil {
		return nil, err
	}
	for i, pod := range pods {
		if c.getRollingUpdateFlagFromPod(&pod) {
			podName := util.NameFromMeta(pods[i].ObjectMeta)
			err := retryutil.Retry(1*time.Second, 5*time.Second,
				func() (bool, error) {
					err2 := c.KubeClient.Pods(podName.Namespace).Delete(
						context.TODO(),
						podName.Name,
						c.deleteOptions)
					if err2 != nil {
						return false, err2
					}
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf("could not delete pooler pod: %v", err)
			}
		}
	}

	if service, err = c.KubeClient.Services(c.Namespace).Get(context.TODO(), connectionPooler.FullName, metav1.GetOptions{}); err == nil {
		connectionPooler.Service = service
		desiredSvc := c.generateConnectionPoolerService(connectionPooler, newConnectionPoolerSpec)
		if match, reason := c.compareServices(service, desiredSvc); !match {
			syncReason = append(syncReason, reason)
			c.logServiceChanges(role, service, desiredSvc, false, reason)
			newService, err = c.updateService(role, service, desiredSvc)
			if err != nil {
				return syncReason, fmt.Errorf("could not update %s service to match desired state: %v", role, err)
			}
			connectionPooler.Service = newService
			c.logger.Infof("%s service %q is in the desired state now", role, util.NameFromMeta(desiredSvc.ObjectMeta))
		}
		return NoSync, nil
	}

	if !k8sutil.ResourceNotFound(err) {
		return NoSync, fmt.Errorf("could not get connection pooler service to sync: %v", err)
	}

	connectionPooler.Service = nil
	c.logger.Warningf("service %s for connection pooler synchronization is not found, create it", connectionPooler.FullName)

	serviceSpec := c.generateConnectionPoolerService(connectionPooler, newConnectionPoolerSpec)
	newService, err = c.KubeClient.
		Services(serviceSpec.Namespace).
		Create(context.TODO(), serviceSpec, metav1.CreateOptions{})

	if err != nil {
		return NoSync, err
	}
	connectionPooler.Service = newService

	return NoSync, nil
}
