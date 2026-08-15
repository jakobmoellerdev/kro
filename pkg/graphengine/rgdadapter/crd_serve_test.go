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

package rgdadapter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	kroclient "github.com/kubernetes-sigs/kro/pkg/client"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

func TestEnsureInstanceCRD_StampsLabelsAndDelegates(t *testing.T) {
	rgd := newNamespacedWebAppRGD()
	rgd.UID = types.UID("webapp-uid")

	stub := &stubCRDClient{store: map[string]*extv1.CustomResourceDefinition{}}
	got, err := EnsureInstanceCRD(context.Background(), stub, rgd, false)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, "webapps.kro.run", got.Name)
	assert.True(t, metadata.IsKROOwned(&got.ObjectMeta))
	assert.Equal(t, rgd.Name, got.Labels[metadata.ResourceGraphDefinitionNameLabel])
	assert.Equal(t, string(rgd.UID), got.Labels[metadata.ResourceGraphDefinitionIDLabel])
	assert.Equal(t, 1, stub.ensureCalls)
	assert.False(t, stub.lastAllowBreaking)
	require.NotEmpty(t, got.Spec.Versions)
	status := got.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"]
	assert.Equal(t, "object", status.Type, "empty-status placeholder must be typed for the apiserver")
	assert.Empty(t, status.Properties)
}

func TestEnsureInstanceCRD_PropagatesEnsureError(t *testing.T) {
	rgd := newNamespacedWebAppRGD()
	rgd.UID = types.UID("webapp-uid")

	stub := &stubCRDClient{
		store:     map[string]*extv1.CustomResourceDefinition{},
		ensureErr: fmt.Errorf("failed to update CRD webapps.kro.run: CRD is owned by another ResourceGraphDefinition other"),
	}
	_, err := EnsureInstanceCRD(context.Background(), stub, rgd, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "owned by another ResourceGraphDefinition")
}

func TestEnsureInstanceCRD_Errors(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		_, err := EnsureInstanceCRD(context.Background(), nil, newNamespacedWebAppRGD(), false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "crd client is required")
	})

	t.Run("nil rgd", func(t *testing.T) {
		_, err := EnsureInstanceCRD(context.Background(), &stubCRDClient{}, nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resourcegraphdefinition is required")
	})
}

func TestDeleteInstanceCRD_SkipsUnowned(t *testing.T) {
	rgd := newNamespacedWebAppRGD()
	stub := &stubCRDClient{
		store: map[string]*extv1.CustomResourceDefinition{
			"webapps.kro.run": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "webapps.kro.run",
					Labels: map[string]string{
						metadata.ResourceGraphDefinitionNameLabel: "other-rgd",
					},
				},
			},
		},
	}

	require.NoError(t, DeleteInstanceCRD(context.Background(), stub, rgd))
	assert.Empty(t, stub.deleted)
	_, err := stub.Get(context.Background(), "webapps.kro.run")
	require.NoError(t, err)
}

func TestDeleteInstanceCRD_DeletesOwned(t *testing.T) {
	rgd := newNamespacedWebAppRGD()
	stub := &stubCRDClient{
		store: map[string]*extv1.CustomResourceDefinition{
			"webapps.kro.run": {
				ObjectMeta: metav1.ObjectMeta{
					Name: "webapps.kro.run",
					Labels: map[string]string{
						metadata.ResourceGraphDefinitionNameLabel: rgd.Name,
					},
				},
			},
		},
	}

	require.NoError(t, DeleteInstanceCRD(context.Background(), stub, rgd))
	assert.Equal(t, []string{"webapps.kro.run"}, stub.deleted)
}

func TestDeleteInstanceCRD_MissingIsNoop(t *testing.T) {
	rgd := newNamespacedWebAppRGD()
	stub := &stubCRDClient{store: map[string]*extv1.CustomResourceDefinition{}}
	require.NoError(t, DeleteInstanceCRD(context.Background(), stub, rgd))
	assert.Empty(t, stub.deleted)
}

func TestDeleteInstanceCRD_Errors(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		err := DeleteInstanceCRD(context.Background(), nil, newNamespacedWebAppRGD())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "crd client is required")
	})

	t.Run("nil rgd", func(t *testing.T) {
		err := DeleteInstanceCRD(context.Background(), &stubCRDClient{}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resourcegraphdefinition is required")
	})

	t.Run("nil schema", func(t *testing.T) {
		err := DeleteInstanceCRD(context.Background(), &stubCRDClient{}, &v1alpha1.ResourceGraphDefinition{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "schema is required")
	})
}

func TestInstanceCRDName(t *testing.T) {
	assert.Equal(t, "webapps.kro.run", instanceCRDName("kro.run", "WebApp"))
	assert.Equal(t, "networkpolicies.example.io", instanceCRDName("example.io", "NetworkPolicy"))
}

type stubCRDClient struct {
	store             map[string]*extv1.CustomResourceDefinition
	ensureErr         error
	ensureCalls       int
	lastAllowBreaking bool
	deleted           []string
}

var _ kroclient.CRDClient = (*stubCRDClient)(nil)

func (s *stubCRDClient) Ensure(_ context.Context, crd extv1.CustomResourceDefinition, allowBreakingChanges bool) error {
	s.ensureCalls++
	s.lastAllowBreaking = allowBreakingChanges
	if s.ensureErr != nil {
		return s.ensureErr
	}
	if s.store == nil {
		s.store = map[string]*extv1.CustomResourceDefinition{}
	}
	stored := crd.DeepCopy()
	s.store[crd.Name] = stored
	return nil
}

func (s *stubCRDClient) Get(_ context.Context, name string) (*extv1.CustomResourceDefinition, error) {
	if crd, ok := s.store[name]; ok {
		return crd.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, name)
}

func (s *stubCRDClient) Delete(_ context.Context, name string) error {
	s.deleted = append(s.deleted, name)
	delete(s.store, name)
	return nil
}
