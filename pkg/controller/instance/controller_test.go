// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package instance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance/applyset"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// TestReconcileInstanceLoad exercises the engine-agnostic instance-load path
// that runs before the deletion/graph-engine branches.
func TestReconcileInstanceLoad(t *testing.T) {
	tests := []struct {
		name    string
		objects []apimachineryruntime.Object
		getErr  string
		request types.NamespacedName
		wantErr string
	}{
		{
			name:    "instance not found",
			request: types.NamespacedName{Name: "missing", Namespace: "default"},
		},
		{
			name:    "load errors are returned",
			objects: []apimachineryruntime.Object{newInstanceObject("demo", "default")},
			getErr:  "get failed",
			request: types.NamespacedName{Name: "demo", Namespace: "default"},
			wantErr: "get failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := newControllerTestDynamicClient(t, tt.objects...)
			if tt.getErr != "" {
				raw.PrependReactor("get", "webapps", func(action k8stesting.Action) (bool, apimachineryruntime.Object, error) {
					return true, nil, errors.New(tt.getErr)
				})
			}

			controller, _ := newControllerUnderTest(t, raw, newTestGraph())
			err := controller.Reconcile(context.Background(), ctrl.Request{NamespacedName: tt.request})

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestReconcileDeletionRemovesFinalizer(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	addEmptyDeletionScope(instance)
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	})
	require.NoError(t, err)

	stored := getStoredParentObject(t, raw)
	assert.False(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, metav1.ConditionUnknown, conditionByType(t, stored, ResourcesReady).Status)
}

func TestReconcileDeletionPreservesAuthorStatusWithoutRuntime(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	addEmptyDeletionScope(instance)
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))
	require.NoError(t, unstructured.SetNestedMap(instance.Object, map[string]interface{}{
		"state":    string(v1alpha1.InstanceStateActive),
		"endpoint": "https://example.test",
		"conditions": []interface{}{map[string]interface{}{
			"type":               "AuthorHealthy",
			"status":             "True",
			"reason":             "Healthy",
			"lastTransitionTime": "2026-01-01T00:00:00Z",
		}},
	}, "status"))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())
	controller.reconcileConfig.HasAuthorConditions = true

	require.NoError(t, controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	}))

	stored := getStoredParentObject(t, raw)
	assert.False(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, "https://example.test", stored.Object["status"].(map[string]interface{})["endpoint"])
	conditions := conditionsFromInstance(stored)
	require.Len(t, conditions, 2)
	authorHealthy := conditionByType(t, stored, "AuthorHealthy")
	require.NotNil(t, authorHealthy.LastTransitionTime)
	assert.Equal(t, "2026-01-01T00:00:00Z", authorHealthy.LastTransitionTime.UTC().Format(time.RFC3339))
	resourcesReady := conditionByType(t, stored, ResourcesReady)
	assert.Equal(t, metav1.ConditionUnknown, resourcesReady.Status)
	assert.Equal(t, new("UnderDeletion"), resourcesReady.Reason)
}

func TestReconcileDeletionSurfacesErrorsWithAuthorConditions(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))
	require.NoError(t, unstructured.SetNestedMap(instance.Object, map[string]interface{}{
		"state": string(v1alpha1.InstanceStateActive),
		"conditions": []interface{}{map[string]interface{}{
			"type":               "AuthorHealthy",
			"status":             "True",
			"reason":             "Healthy",
			"lastTransitionTime": "2026-01-01T00:00:00Z",
		}},
	}, "status"))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())
	controller.reconcileConfig.HasAuthorConditions = true

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	})
	require.Error(t, err)

	stored := getStoredParentObject(t, raw)
	assert.True(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, metav1.ConditionTrue, conditionByType(t, stored, "AuthorHealthy").Status)
	resourcesReady := conditionByType(t, stored, ResourcesReady)
	assert.Equal(t, metav1.ConditionUnknown, resourcesReady.Status)
	require.NotNil(t, resourcesReady.Message)
	assert.Contains(t, *resourcesReady.Message, "deletion blocked")
	assert.Contains(t, *resourcesReady.Message, applyset.ApplySetParentIDLabel)
}
