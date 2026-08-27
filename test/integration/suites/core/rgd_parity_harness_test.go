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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// This file holds the package-level parity harness shared by every
// rgd_parity_*_test.go file. Each ported RGD scenario is a DescribeTable whose
// two Entries run the SAME fixture through both backends:
//
//	Entry("builtin",        parityBuiltin)
//	Entry("graph-v1alpha2", parityGraph)
//
// and whose body calls runParityFixture(...). Keeping the harness package-level
// lets scenario files be authored independently (no shared Describe closure), so
// porting can be split across many files without merge conflicts.

// parityBuiltin / parityGraph are the backend constructors passed to each
// DescribeTable Entry. The graph backend carries per-run state (its isolating
// group), so it is constructed fresh per entry.
func parityBuiltin() rgdBackend { return builtinRGDBackend{} }
func parityGraph() rgdBackend   { return newGraphRGDBackend() }

// runParityFixture drives one RGD fixture through one backend end to end:
// deploy the definition, create an instance, run the caller's assertions, then
// assert the instance reaches the backend's ready state.
//
//   - makeRGD builds the RGD (name is a unique per-run prefix so the two
//     backends' cluster-scoped CRDs never collide).
//   - instanceSpec is the created instance's spec.
//   - assertChildren asserts the shared contract; it receives the test handle,
//     the namespace, the instance name, the resolved instance GVK, and the
//     backend (so backend-specific assertions can branch on be.Name()).
//
// Pass skipReadyAssert=true for scenarios that intentionally never converge
// (e.g. data-pending / invalid), asserting readiness themselves instead.
func runParityFixture(
	be rgdBackend,
	makeRGD func(name string) *krov1alpha1.ResourceGraphDefinition,
	instanceSpec map[string]any,
	assertChildren func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend),
) {
	runParityFixtureOpts(be, makeRGD, instanceSpec, assertChildren, false)
}

// runParityFixtureOpts is runParityFixture with an explicit skip-ready flag.
func runParityFixtureOpts(
	be rgdBackend,
	makeRGD func(name string) *krov1alpha1.ResourceGraphDefinition,
	instanceSpec map[string]any,
	assertChildren func(t GinkgoTInterface, ns, instanceName string, gvk schema.GroupVersionKind, be rgdBackend),
	skipReadyAssert bool,
) {
	t := GinkgoT()
	ns := createParityNamespace(t)

	rgd := makeRGD("parity" + rand.String(5))
	gvk := be.Deploy(t, rgd)

	instName := "inst-" + rand.String(5)
	inst := newInstanceCR(gvk, ns, instName, instanceSpec)
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := env.Client.Create(ctx, inst); err != nil {
		t.Fatalf("%s: create instance: %v", be.Name(), err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), inst) })

	if assertChildren != nil {
		assertChildren(t, ns, instName, gvk, be)
	}

	if !skipReadyAssert {
		awaitInstanceReady(t, be, gvk, types.NamespacedName{Namespace: ns, Name: instName})
	}
}

// createParityNamespace makes a throwaway namespace for a parity spec and
// registers cleanup.
func createParityNamespace(t GinkgoTInterface) string {
	t.Helper()
	name := fmt.Sprintf("parity-%s", rand.String(6))
	ctx := env.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := env.Client.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	t.Cleanup(func() { _ = env.Client.Delete(context.Background(), ns) })
	return name
}

// awaitInstanceReady asserts the instance converges to a ready state under the
// backend's own status convention: the built-in controller sets
// status.state=ACTIVE, the graph backend sets status.ready=true.
func awaitInstanceReady(t GinkgoTInterface, be rgdBackend, gvk schema.GroupVersionKind, key types.NamespacedName) {
	t.Helper()
	awaitInstanceCR(t, gvk, key, func(u *unstructured.Unstructured) error {
		if be.Name() == (builtinRGDBackend{}).Name() {
			state, _, _ := unstructured.NestedString(u.Object, "status", "state")
			if state != string(krov1alpha1.ResourceGraphDefinitionStateActive) && state != "ACTIVE" {
				return &notYetError{msg: "status.state=" + state + ", want ACTIVE"}
			}
			return nil
		}
		ready, found, _ := unstructured.NestedBool(u.Object, "status", "ready")
		if !found || !ready {
			return &notYetError{msg: fmt.Sprintf("status.ready=%v found=%v, want true", ready, found)}
		}
		return nil
	}, 60*time.Second)
}

// isBuiltin reports whether be is the built-in RGD backend, for scenarios that
// branch a backend-specific assertion (e.g. strict readiness gating, RGD status
// shapes) that has no standalone-Graph analog.
func isBuiltin(be rgdBackend) bool { return be.Name() == (builtinRGDBackend{}).Name() }

// GVKs commonly asserted by ported fixtures.
var (
	deploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	serviceGVK    = schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	secretGVK     = schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
)
