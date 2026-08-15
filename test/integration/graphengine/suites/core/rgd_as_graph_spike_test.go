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
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/test/integration/graphengine/environment"
)

// TestRGDAsGraphSpike is the Phase-3 decision-gate canary: can the
// simplest RGD composition — two templates, one CEL-wired to the
// other — be expressed as a Graph and reconciled by the Graph engine
// to the same effect (both resources applied, the reference resolved,
// both entries tracked)?
//
// This is not an RGD translation layer. It is a hand-authored Graph
// that a trivial RGD (ConfigMap `cm` + a second ConfigMap whose data
// references `${cm.metadata.name}`) would compile down to, if RGD
// flipped onto the Graph engine. If this fails, RGD-from-Graph is
// blocked on the engine itself, not on CRD/instance plumbing.
func TestRGDAsGraphSpike(t *testing.T) {
	env := environment.Shared(t)
	ns := env.CreateNamespace(t)

	g := &expv1alpha1.Graph{
		ObjectMeta: metav1.ObjectMeta{Name: "rgd-spike", Namespace: ns},
		Spec: expv1alpha1.GraphSpec{
			Nodes: []expv1alpha1.Node{
				{
					ID: "cm",
					Template: environment.RawExt(t, map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "spike-cm"},
						"data":       map[string]any{"hello": "world"},
					}),
				},
				{
					ID: "alias",
					Template: environment.RawExt(t, map[string]any{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "spike-alias"},
						"data":       map[string]any{"from": "${cm.metadata.name}"},
					}),
				},
			},
		},
	}
	env.CreateGraph(t, g)

	key := types.NamespacedName{Namespace: ns, Name: "rgd-spike"}
	env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 15*time.Second)
	env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 15*time.Second)

	env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "spike-cm"}, func(u *unstructured.Unstructured) error {
		v, _, _ := unstructured.NestedString(u.Object, "data", "hello")
		if v != "world" {
			return fmt.Errorf("cm data.hello=%q want world", v)
		}
		return nil
	}, 15*time.Second)

	env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "spike-alias"}, func(u *unstructured.Unstructured) error {
		v, _, _ := unstructured.NestedString(u.Object, "data", "from")
		if v != "spike-cm" {
			return fmt.Errorf("alias data.from=%q want spike-cm (CEL ${cm.metadata.name} did not resolve)", v)
		}
		return nil
	}, 15*time.Second)

	environment.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
		got := env.GetGraph(t, key)
		if len(got.Status.ManagedResources) != 2 {
			return fmt.Errorf("status.managedResources len=%d want 2", len(got.Status.ManagedResources))
		}
		seen := map[string]expv1alpha1.ManagedResource{}
		for _, e := range got.Status.ManagedResources {
			seen[e.NodeID] = e
		}
		cm, ok := seen["cm"]
		if !ok {
			return fmt.Errorf("managedResources missing nodeID=cm: %+v", got.Status.ManagedResources)
		}
		if cm.APIVersion != "v1" || cm.Kind != "ConfigMap" || cm.Name != "spike-cm" || cm.Namespace != ns || cm.UID == "" {
			return fmt.Errorf("cm tracking=%+v want v1/ConfigMap %s/spike-cm with UID", cm, ns)
		}
		alias, ok := seen["alias"]
		if !ok {
			return fmt.Errorf("managedResources missing nodeID=alias: %+v", got.Status.ManagedResources)
		}
		if alias.APIVersion != "v1" || alias.Kind != "ConfigMap" || alias.Name != "spike-alias" || alias.Namespace != ns || alias.UID == "" {
			return fmt.Errorf("alias tracking=%+v want v1/ConfigMap %s/spike-alias with UID", alias, ns)
		}
		return nil
	})
}
