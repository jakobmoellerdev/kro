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

package core_test

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// rgdBackend abstracts "how an RGD definition becomes a running controller"
// so one behavioral fixture can be validated against more than one
// implementation:
//
//   - builtinRGDBackend: the built-in ResourceGraphDefinition controller
//     (create the RGD object; the controller synthesizes the CRD and watches
//     instances).
//   - graphRGDBackend: the RGD-as-Graph controller from examples/graph/rgd.yaml,
//     served at v1alpha2 under a distinct group so it never collides with the
//     built-in kro.run RGD CRD/controller.
//
// The two share an observable contract: given the same RGD schema + resources
// and the same instance spec, the same child resources converge with the same
// resolved field values. The abstraction exposes only what a behavioral test
// needs — deploy the definition, create an instance, and address the instance's
// GVK — never backend-internal status shapes (TopologicalOrder, RGD conditions),
// which differ between implementations.
type rgdBackend interface {
	// Name identifies the backend in spec output (Ginkgo table entries).
	Name() string

	// Deploy realizes rgd's definition so its Kind CRD exists and is watched.
	// Called once per fixture. The returned GVK is what CreateInstance and the
	// caller use to address instances of the user's Kind (its group/version may
	// differ from the fixture's for an isolating backend).
	Deploy(t environment.TestingT, rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind

	// InstanceGVK returns the GVK instances of rgd's Kind are created under for
	// this backend. Valid after Deploy.
	InstanceGVK(rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind
}

// awaitInstanceCR polls until the named instance exists and match returns nil.
// Shared by the parameterized fixtures to assert convergence regardless of
// backend.
func awaitInstanceCR(
	t environment.TestingT,
	gvk schema.GroupVersionKind,
	key types.NamespacedName,
	match func(*unstructured.Unstructured) error,
	timeout time.Duration,
) *unstructured.Unstructured {
	return env.AwaitObject(t, gvk, key, match, timeout)
}

// newInstanceCR builds an unstructured instance of gvk with the given spec.
func newInstanceCR(gvk schema.GroupVersionKind, ns, name string, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName(name)
	u.SetNamespace(ns)
	if spec != nil {
		u.Object["spec"] = spec
	}
	return u
}

// ── built-in RGD backend ──────────────────────────────────────────────────

// builtinRGDBackend realizes an RGD through the built-in controller: it creates
// the ResourceGraphDefinition object and lets the running controller synthesize
// the Kind CRD and reconcile instances. This is the reference implementation
// the graph backend is compared against.
type builtinRGDBackend struct{}

func (builtinRGDBackend) Name() string { return "builtin-rgd" }

func (b builtinRGDBackend) Deploy(t environment.TestingT, rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind {
	t.Helper()
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := env.Client.Create(ctx, rgd); err != nil {
		t.Fatalf("builtin backend: create RGD %q: %v", rgd.Name, err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), rgd) })

	gvk := b.InstanceGVK(rgd)
	// Wait for the controller to synthesize and establish the Kind CRD.
	environment.Eventually(t, 60*time.Second, 500*time.Millisecond, func() error {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{
			Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List",
		})
		return env.Client.List(ctx, list)
	})
	return gvk
}

func (builtinRGDBackend) InstanceGVK(rgd *krov1alpha1.ResourceGraphDefinition) schema.GroupVersionKind {
	group := rgd.Spec.Schema.Group
	if group == "" {
		group = krov1alpha1.KRODomainName
	}
	return schema.GroupVersionKind{
		Group:   group,
		Version: rgd.Spec.Schema.APIVersion,
		Kind:    rgd.Spec.Schema.Kind,
	}
}

var _ = metav1.ObjectMeta{}
