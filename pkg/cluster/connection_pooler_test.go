package cluster

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func mockInstallLookupFunction(schema string, user string) error {
	return nil
}

func boolToPointer(value bool) *bool {
	return &value
}

func deploymentUpdated(cluster *Cluster, err error, reason SyncReason, poolers map[PostgresRole][]string) error {
	for role, poolerNames := range poolers {
		for _, poolerName := range poolerNames {
			poolerLabels := cluster.labelsSet(false)
			poolerLabels["application"] = "db-connection-pooler"
			poolerLabels["connection-pooler"] = cluster.connectionPoolerFullName(role, poolerName)

			if cluster.ConnectionPoolers != nil && cluster.ConnectionPoolers.Objects[poolerName] != nil && cluster.ConnectionPoolers.Objects[poolerName].Deployment != nil &&
				util.MapContains(cluster.ConnectionPoolers.Objects[poolerName].Deployment.Labels, poolerLabels) &&
				(cluster.ConnectionPoolers.Objects[poolerName].Deployment.Spec.Replicas == nil ||
					*cluster.ConnectionPoolers.Objects[poolerName].Deployment.Spec.Replicas != 2) {
				return fmt.Errorf("Wrong number of instances")
			}
		}
	}
	return nil
}

func checkPoolersSync(cluster *Cluster, err error, reason SyncReason, expectedPoolers map[PostgresRole]map[string]struct{}) error {
	if len(expectedPoolers[Master]) == 0 && len(expectedPoolers[Replica]) == 0 {
		if cluster.ConnectionPoolers == nil || cluster.ConnectionPoolers.Objects == nil || len(cluster.ConnectionPoolers.Objects) == 0 {
			return nil
		} else {
			return fmt.Errorf("Expected no connection poolers but some exist")
		}
	}

	var poolerFound int

	for role, poolerNames := range expectedPoolers {
		for poolerName := range poolerNames {
			pooler, ok := cluster.ConnectionPoolers.Objects[poolerName]
			if !ok || pooler.Role != role {
				return fmt.Errorf("Connection pooler %s for role %s should exist", poolerName, role)
			}

			poolerFound++

			poolerLabels := cluster.labelsSet(false)
			poolerLabels["application"] = "db-connection-pooler"
			poolerLabels["connection-pooler"] = cluster.connectionPoolerFullName(pooler.Role, poolerName)

			if pooler.Deployment == nil || !util.MapContains(pooler.Deployment.Labels, poolerLabels) {
				return fmt.Errorf("Deployment was not saved or labels not attached %s '%s' %s", pooler.Role, poolerName, pooler.Deployment)
			}

			if pooler.Service == nil || !util.MapContains(pooler.Service.Labels, poolerLabels) {
				return fmt.Errorf("Service was not saved or labels not attached '%s' %s", poolerName, pooler.Service.Labels)
			}

		}
	}

	if poolerFound < len(cluster.ConnectionPoolers.Objects) {
		return errors.New("Some unexpected poolers exist")
	}

	return nil
}

func objectsAreDeleted(cluster *Cluster, err error, reason SyncReason, poolers map[PostgresRole][]string) error {
	for role, poolerNames := range poolers {
		for _, poolerName := range poolerNames {
			if cluster.ConnectionPoolers != nil && cluster.ConnectionPoolers.Objects[poolerName] != nil &&
				(cluster.ConnectionPoolers.Objects[poolerName].Deployment != nil || cluster.ConnectionPoolers.Objects[poolerName].Service != nil) {
				return fmt.Errorf("Connection pooler '%s' was not deleted for role %v", poolerName, role)
			}
		}
	}

	return nil
}

func noEmptySync(cluster *Cluster, err error, reason SyncReason, poolers map[PostgresRole]map[string]struct{}) error {
	for _, msg := range reason {
		if strings.HasPrefix(msg, "update [] from '<nil>' to '") {
			return fmt.Errorf("There is an empty reason, %s", msg)
		}
	}

	return nil
}

func TestNeedConnectionPooler(t *testing.T) {
	testName := "Test how connection pooler can be enabled"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.NewMockKubernetesClient(), acidv1.Postgresql{}, logger, eventRecorder)

	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler: &acidv1.ConnectionPooler{},
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with full definition",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(true),
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with flag",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(false),
		ConnectionPooler:       &acidv1.ConnectionPooler{},
	}

	if needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is still enabled with flag being false",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(true),
		ConnectionPooler:       &acidv1.ConnectionPooler{},
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with flag and full",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler:        boolToPointer(false),
		EnableReplicaConnectionPooler: boolToPointer(false),
		ConnectionPooler:              nil,
	}

	if needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is enabled with flag false and nil",
			testName)
	}

	// Test for replica connection pooler
	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler: &acidv1.ConnectionPooler{},
	}

	if needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with full definition",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(true),
	}

	if !needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with flag",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(false),
		ConnectionPooler:              &acidv1.ConnectionPooler{},
	}

	if needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is still enabled with flag being false",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(true),
		ConnectionPooler:              &acidv1.ConnectionPooler{},
	}

	if !needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with flag and full",
			testName)
	}
}

func TestConnectionPoolerCreateDeletion(t *testing.T) {

	testName := "test connection pooler creation and deletion"
	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()
	namespace := "default"

	client := k8sutil.KubernetesClient{
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acid-fake-cluster",
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(true),
			EnableReplicaConnectionPooler: boolToPointer(true),
			ConnectionPoolers: map[string]*acidv1.ConnectionPoolerParameters{
				"foo": &acidv1.ConnectionPoolerParameters{
					Target: string(Master),
				},
				"bar": &acidv1.ConnectionPoolerParameters{
					Target: string(Replica),
				},
			},
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     "cluster-name",
					DefaultCPURequest:    "300m",
					DefaultCPULimit:      "300m",
					DefaultMemoryRequest: "300Mi",
					DefaultMemoryLimit:   "300Mi",
					PodRoleLabel:         "spilo-role",
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = "acid-fake-cluster"
	cluster.Namespace = "default"

	_, err := cluster.createService(Master)
	assert.NoError(t, err)
	_, err = cluster.createStatefulSet()
	assert.NoError(t, err)

	reason, err := cluster.createConnectionPooler(mockInstallLookupFunction)

	if err != nil {
		t.Errorf("%s: Cannot create connection pooler, %s, %+v",
			testName, err, reason)
	}

	for poolerName, poolerParams := range pg.Spec.ConnectionPoolers {
		role := PostgresRole(poolerParams.Target)
		poolerFullName := cluster.connectionPoolerFullName(role, poolerName)
		poolerLabels := cluster.labelsSet(false)
		poolerLabels["application"] = "db-connection-pooler"
		poolerLabels["connection-pooler"] = poolerFullName

		if cluster.ConnectionPoolers != nil {
			if cluster.ConnectionPoolers.Objects[poolerName] == nil || (cluster.ConnectionPoolers.Objects[poolerName].Deployment == nil && util.MapContains(cluster.ConnectionPoolers.Objects[poolerName].Deployment.Labels, poolerLabels)) {
				t.Errorf("%s: Connection pooler deployment is empty for role %s and pooler %s", testName, role, poolerName)
			}

			if cluster.ConnectionPoolers.Objects[poolerName] == nil || (cluster.ConnectionPoolers.Objects[poolerName].Service == nil && util.MapContains(cluster.ConnectionPoolers.Objects[poolerName].Service.Labels, poolerLabels)) {
				t.Errorf("%s: Connection pooler service is empty for role %s", testName, role)
			}
		} else {
			t.Errorf("Connection poolers is empty")
		}
	}

	oldSpec := &acidv1.Postgresql{
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(true),
			EnableReplicaConnectionPooler: boolToPointer(true),
		},
	}
	newSpec := &acidv1.Postgresql{
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(false),
			EnableReplicaConnectionPooler: boolToPointer(false),
		},
	}

	// Delete connection pooler via sync
	_, err = cluster.syncConnectionPooler(oldSpec, newSpec, mockInstallLookupFunction)
	if err != nil {
		t.Errorf("%s: Cannot sync connection pooler, %s", testName, err)
	}

	for poolerName, poolerParams := range pg.Spec.ConnectionPoolers {
		role := PostgresRole(poolerParams.Target)
		poolerName = cluster.connectionPoolerFullName(role, poolerName)
		if cluster.ConnectionPoolers.Objects[poolerName] != nil {
			t.Errorf("%s: Connection pooler %s should have been deleted but still exists", testName, poolerName)
		}
	}
}

func TestConnectionPoolerSync(t *testing.T) {

	testName := "test connection pooler synchronization"
	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()
	namespace := "default"

	client := k8sutil.KubernetesClient{
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acid-fake-cluster",
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     "cluster-name",
					DefaultCPURequest:    "300m",
					DefaultCPULimit:      "300m",
					DefaultMemoryRequest: "300Mi",
					DefaultMemoryLimit:   "300Mi",
					PodRoleLabel:         "spilo-role",
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = "acid-fake-cluster"
	cluster.Namespace = "default"

	_, err := cluster.createService(Master)
	assert.NoError(t, err)
	_, err = cluster.createStatefulSet()
	assert.NoError(t, err)

	reason, err := cluster.createConnectionPooler(mockInstallLookupFunction)

	if err != nil {
		t.Errorf("%s: Cannot create connection pooler, %s, %+v",
			testName, err, reason)
	}

	tests := []struct {
		subTest          string
		oldSpec          *acidv1.Postgresql
		newSpec          *acidv1.Postgresql
		cluster          *Cluster
		defaultImage     string
		defaultInstances int32
		expectedPoolers  map[PostgresRole]map[string]struct{}
		check            func(cluster *Cluster, err error, reason SyncReason, poolers map[PostgresRole]map[string]struct{}) error
	}{
		{
			subTest: "create from scratch only master",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete only master",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(false),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "create master and replica",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(true),
					ConnectionPooler:              &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
				Replica: map[string]struct{}{PoolerDefaultReplicaKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete master and replica",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(false),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "create if doesn't exist",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete master and replica",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(false),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "create if doesn't exist with a flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler:        boolToPointer(true),
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
				Replica: map[string]struct{}{PoolerDefaultReplicaKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "create no replica with flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(false),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "create replica if doesn't exist with a flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
				Replica: map[string]struct{}{PoolerDefaultReplicaKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "create both master and replica",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
					EnableConnectionPooler:        boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
				Replica: map[string]struct{}{PoolerDefaultReplicaKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete only replica if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete only master if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:       &acidv1.ConnectionPooler{},
					EnableConnectionPooler: boolToPointer(true),
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Replica: map[string]struct{}{PoolerDefaultReplicaKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "delete if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "cleanup if still there",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers:  map[PostgresRole]map[string]struct{}{},
			check:            checkPoolersSync,
		},
		{
			subTest: "update image from changed defaults",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "add additional master pooler",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
					ConnectionPoolers: map[string]*acidv1.ConnectionPoolerParameters{
						"foo": &acidv1.ConnectionPoolerParameters{
							Target: string(Master),
						},
					},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}, "foo": struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "add additional replica pooler",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
					ConnectionPoolers: map[string]*acidv1.ConnectionPoolerParameters{
						"foo": &acidv1.ConnectionPoolerParameters{
							Target: string(Master),
						},
						"bar": &acidv1.ConnectionPoolerParameters{
							Target: string(Replica),
						},
					},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{PoolerDefaultMasterKey: struct{}{}, "foo": struct{}{}},
				Replica: map[string]struct{}{"bar": struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "disable default pooler",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					DisableDefaultPooler: boolToPointer(true),
					ConnectionPooler:     &acidv1.ConnectionPooler{},
					ConnectionPoolers: map[string]*acidv1.ConnectionPoolerParameters{
						"foo": &acidv1.ConnectionPoolerParameters{
							Target: string(Master),
						},
						"bar": &acidv1.ConnectionPoolerParameters{
							Target: string(Replica),
						},
					},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{"foo": struct{}{}},
				Replica: map[string]struct{}{"bar": struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "removing additional replica and master pooler",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master: map[string]struct{}{PoolerDefaultMasterKey: struct{}{}},
			},
			check: checkPoolersSync,
		},
		{
			subTest: "there is no sync from nil to an empty spec",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(true),
					ConnectionPooler:       nil,
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(true),
					ConnectionPooler:       &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			expectedPoolers: map[PostgresRole]map[string]struct{}{
				Master:  map[string]struct{}{"": struct{}{}},
				Replica: map[string]struct{}{"": struct{}{}},
			},
			check: noEmptySync,
		},
	}
	for _, tt := range tests {
		t.Run(tt.subTest, func(t *testing.T) {
			tt.cluster.OpConfig.ConnectionPooler.Image = tt.defaultImage
			tt.cluster.OpConfig.ConnectionPooler.NumberOfInstances =
				k8sutil.Int32ToPointer(tt.defaultInstances)

			t.Logf("running test for %s [%s]", testName, tt.subTest)

			reason, err := tt.cluster.syncConnectionPooler(tt.oldSpec,
				tt.newSpec, mockInstallLookupFunction)

			if err := tt.check(tt.cluster, err, reason, tt.expectedPoolers); err != nil {
				t.Errorf("%s [%s]: Could not synchronize, %+v",
					testName, tt.subTest, err)
			}
		})
	}
}

func TestConnectionPoolerPodSpec(t *testing.T) {
	testName := "Test connection pooler pod template generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					MaxDBConnections:                     k8sutil.Int32ToPointer(60),
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler:              &acidv1.ConnectionPooler{},
		EnableReplicaConnectionPooler: boolToPointer(true),
	}
	var clusterNoDefaultRes = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	clusterNoDefaultRes.Spec = acidv1.PostgresSpec{
		ConnectionPooler:              &acidv1.ConnectionPooler{},
		EnableReplicaConnectionPooler: boolToPointer(true),
	}

	clusterNoDefaultRes.ConnectionPoolers = &ConnectionPoolers{}
	cluster.ConnectionPoolers = &ConnectionPoolers{}

	noCheck := func(_ *Cluster, _ *v1.PodTemplateSpec, _ PostgresRole, _ string) error { return nil }

	tests := []struct {
		subTest          string
		spec             *acidv1.PostgresSpec
		poolerParameters acidv1.ConnectionPoolerParameters
		poolerName       string
		expected         error
		cluster          *Cluster

		check func(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole, poolerName string) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			poolerParameters: acidv1.ConnectionPoolerParameters{},
			expected:         nil,
			cluster:          cluster,
			check:            noCheck,
		},
		{
			subTest: "no default resources",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			poolerParameters: acidv1.ConnectionPoolerParameters{},
			expected:         errors.New(`could not fill resource requests: could not parse default CPU quantity: quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'`),
			cluster:          clusterNoDefaultRes,
			check:            noCheck,
		},
		{
			subTest: "default resources are set",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			poolerParameters: acidv1.ConnectionPoolerParameters{},
			expected:         nil,
			cluster:          cluster,
			check:            testResources,
		},
		{
			subTest: "labels for service",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			poolerParameters: acidv1.ConnectionPoolerParameters{},
			expected:         nil,
			cluster:          cluster,
			check:            testLabels,
		},
		{
			subTest: "required envs",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			poolerParameters: acidv1.ConnectionPoolerParameters{},
			expected:         nil,
			cluster:          cluster,
			check:            testEnvs,
		},
	}
	for _, role := range [2]PostgresRole{Master, Replica} {
		for _, tt := range tests {

			poolerSpec, err := tt.cluster.buildConnectionPoolerSpec(&tt.cluster.Spec, role, &tt.poolerParameters)
			if err != nil {
				if err != tt.expected && err.Error() != tt.expected.Error() {
					t.Errorf("%s [%s]: failed to build pooler specs %s", testName, tt.subTest, err)
				}
				continue
			}

			podSpec, err := tt.cluster.generateConnectionPoolerPodTemplate(role, tt.poolerName, poolerSpec, nil)

			if err != tt.expected && err.Error() != tt.expected.Error() {
				t.Errorf("%s [%s]: Could not generate pod template,\n %+v, expected\n %+v",
					testName, tt.subTest, err, tt.expected)
			}

			err = tt.check(cluster, podSpec, role, tt.poolerName)
			if err != nil {
				t.Errorf("%s [%s]: Pod spec is incorrect, %+v",
					testName, tt.subTest, err)
			}
		}
	}
}

// TODO: fix
func TestConnectionPoolerDeploymentSpec(t *testing.T) {
	testName := "Test connection pooler deployment spec generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	cluster.Statefulset = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-sts",
		},
	}
	cluster.ConnectionPoolers = &ConnectionPoolers{
		LookupFunction: true,
		Objects: map[string]*ConnectionPoolerObjects{
			"": {
				Deployment: nil,
				Service:    nil,
				FullName:   "",
				Role:       Master,
			},
		},
	}

	noCheck := func(cluster *Cluster, deployment *appsv1.Deployment, poolerName string) error {
		return nil
	}

	connectionPoolerSpec, _ := cluster.buildConnectionPoolerSpec(
		&cluster.Spec,
		Master,
		&acidv1.ConnectionPoolerParameters{},
	)

	tests := []struct {
		subTest                 string
		spec                    *acidv1.PostgresSpec
		expected                error
		cluster                 *Cluster
		connectionPoolerObjects *ConnectionPoolerObjects
		check                   func(cluster *Cluster, deployment *appsv1.Deployment, poolerName string) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Namespace: "default",
				Role:      Master,
				Spec:      connectionPoolerSpec,
			},
			cluster: cluster,
			check:   noCheck,
		},
		{
			subTest: "owner reference",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Namespace: "default",
				Role:      Master,
				Spec:      connectionPoolerSpec,
			},
			cluster: cluster,
			check: func(cluster *Cluster, deployment *appsv1.Deployment, _ string) error {
				return testDeploymentOwnerReference(cluster, deployment)
			},
		},
		{
			subTest: "selector",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Name:      "foo",
				Namespace: "default",
				Role:      Master,
				Spec:      connectionPoolerSpec,
			},
			cluster: cluster,
			check:   testSelector,
		},
	}
	for _, tt := range tests {

		deployment, err := tt.cluster.generateConnectionPoolerDeployment(tt.connectionPoolerObjects, tt.connectionPoolerObjects.Spec)

		if err != tt.expected && err.Error() != tt.expected.Error() {
			t.Errorf("%s [%s]: Could not generate deployment spec,\n %+v, expected\n %+v",
				testName, tt.subTest, err, tt.expected)
		}

		err = tt.check(cluster, deployment, tt.connectionPoolerObjects.FullName)
		if err != nil {
			t.Errorf("%s [%s]: Deployment spec is incorrect, %+v",
				testName, tt.subTest, err)
		}
	}
}

func testResources(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole, _ string) error {
	cpuReq := podSpec.Spec.Containers[0].Resources.Requests["cpu"]
	if cpuReq.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPURequest {
		return fmt.Errorf("CPU request does not match, got %s, expected %s",
			cpuReq.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPURequest)
	}

	memReq := podSpec.Spec.Containers[0].Resources.Requests["memory"]
	if memReq.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest {
		return fmt.Errorf("Memory request does not match, got %s, expected %s",
			memReq.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest)
	}

	cpuLim := podSpec.Spec.Containers[0].Resources.Limits["cpu"]
	if cpuLim.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPULimit {
		return fmt.Errorf("CPU limit does not match, got %s, expected %s",
			cpuLim.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPULimit)
	}

	memLim := podSpec.Spec.Containers[0].Resources.Limits["memory"]
	if memLim.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit {
		return fmt.Errorf("Memory limit does not match, got %s, expected %s",
			memLim.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit)
	}

	return nil
}

func testLabels(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole, poolerName string) error {
	poolerLabels := podSpec.ObjectMeta.Labels["connection-pooler"]
	connectionPooler := &ConnectionPoolerObjects{Role: role, FullName: poolerName, Spec: &ConnectionPoolerSpec{}}
	if poolerLabels != cluster.connectionPoolerLabels(connectionPooler, connectionPooler.Spec, true).MatchLabels["connection-pooler"] {
		return fmt.Errorf("Pod labels do not match, got %+v, expected %+v",
			podSpec.ObjectMeta.Labels, cluster.connectionPoolerLabels(connectionPooler, connectionPooler.Spec, true).MatchLabels)
	}

	return nil
}

func testSelector(cluster *Cluster, deployment *appsv1.Deployment, poolerName string) error {
	labels := deployment.Spec.Selector.MatchLabels
	connectionPooler := &ConnectionPoolerObjects{Role: Master, FullName: poolerName, Spec: &ConnectionPoolerSpec{}}
	expected := cluster.connectionPoolerLabels(connectionPooler, connectionPooler.Spec, true).MatchLabels

	if labels["connection-pooler"] != expected["connection-pooler"] {
		return fmt.Errorf("Labels are incorrect, got %+v, expected %+v",
			labels, expected)
	}

	return nil
}

func testServiceSelector(cluster *Cluster, service *v1.Service, role PostgresRole, poolerName string) error {
	selector := service.Spec.Selector

	if selector["connection-pooler"] != poolerName {
		return fmt.Errorf("Selector is incorrect, got %s, expected %s",
			selector["connection-pooler"], poolerName)
	}

	return nil
}

func TestConnectionPoolerServiceSpec(t *testing.T) {
	testName := "Test connection pooler service spec generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	cluster.Statefulset = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-sts",
		},
	}

	noCheck := func(cluster *Cluster, deployment *v1.Service, role PostgresRole, poolerName string) error {
		return nil
	}

	tests := []struct {
		subTest                 string
		spec                    *acidv1.PostgresSpec
		cluster                 *Cluster
		connectionPoolerObjects *ConnectionPoolerObjects
		connectionPoolerSpec    *ConnectionPoolerSpec
		check                   func(cluster *Cluster, service *v1.Service, role PostgresRole, poolerName string) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Namespace: "default",
				Role:      Master,
			},
			connectionPoolerSpec: &ConnectionPoolerSpec{},
			cluster:              cluster,
			check:                noCheck,
		},
		{
			subTest: "owner reference",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Namespace: "default",
				Role:      Master,
			},
			connectionPoolerSpec: &ConnectionPoolerSpec{},
			cluster:              cluster,
			check: func(cluster *Cluster, service *v1.Service, role PostgresRole, _ string) error {
				return testServiceOwnerReference(cluster, service, role)
			},
		},
		{
			subTest: "selector",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			connectionPoolerObjects: &ConnectionPoolerObjects{
				FullName:  cluster.connectionPoolerFullName(Master, "foo"),
				Namespace: "default",
				Role:      Master,
			},
			connectionPoolerSpec: &ConnectionPoolerSpec{},
			cluster:              cluster,
			check:                testServiceSelector,
		},
	}
	for _, role := range [2]PostgresRole{Master, Replica} {
		for _, tt := range tests {
			service := tt.cluster.generateConnectionPoolerService(tt.connectionPoolerObjects, tt.connectionPoolerSpec)

			if err := tt.check(cluster, service, role, tt.connectionPoolerObjects.FullName); err != nil {
				t.Errorf("%s [%s]: Service spec is incorrect, %+v",
					testName, tt.subTest, err)
			}
		}
	}
}
