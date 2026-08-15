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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/test/integration/graphengine/environment"
)

// TestPatchContributesAndReleases drives the standalone Graph controller
// through the full patch lifecycle: a patch node contributes a field to a
// pre-existing ConfigMap it does not own, the contribution inventory is
// persisted on the Graph, and removing the patch node from the spec releases
// the contributed field while the target object survives.
func TestPatchContributesAndReleases(t *testing.T) {
	env := environment.Shared(t)
	ns := env.CreateNamespace(t)

	cmGVK := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}
	cmKey := types.NamespacedName{Namespace: ns, Name: "patch-target"}

	// A ConfigMap owned by nobody in this Graph — the patch contributes to it.
	target := &unstructured.Unstructured{}
	target.SetGroupVersionKind(cmGVK)
	target.SetNamespace(ns)
	target.SetName("patch-target")
	if err := unstructured.SetNestedStringMap(target.Object, map[string]string{"orig": "kept"}, "data"); err != nil {
		t.Fatalf("set target data: %v", err)
	}
	if err := env.Client.Create(env.Ctx, target); err != nil {
		t.Fatalf("create target ConfigMap: %v", err)
	}

	g := &expv1alpha1.Graph{
		ObjectMeta: metav1.ObjectMeta{Name: "patcher", Namespace: ns},
		Spec: expv1alpha1.GraphSpec{
			Nodes: []expv1alpha1.Node{
				{
					ID:  "keep",
					Def: environment.RawExt(t, map[string]any{"x": 1}),
				},
				{
					ID: "p",
					Patch: &expv1alpha1.PatchSpec{
						APIVersion: "v1",
						Kind:       "ConfigMap",
						Metadata:   expv1alpha1.PatchMetadata{Name: "patch-target"},
						Body:       environment.RawExt(t, map[string]any{"data": map[string]any{"added": "contributed"}}),
					},
				},
			},
		},
	}
	env.CreateGraph(t, g)

	gKey := types.NamespacedName{Namespace: ns, Name: "patcher"}
	env.AwaitCondition(t, gKey, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 15*time.Second)

	// The contributed field is present; the pre-existing field survives.
	env.AwaitObject(t, cmGVK, cmKey, func(u *unstructured.Unstructured) error {
		data, _, _ := unstructured.NestedStringMap(u.Object, "data")
		if data["added"] != "contributed" {
			return fmt.Errorf("data.added: want=contributed got=%q", data["added"])
		}
		if data["orig"] != "kept" {
			return fmt.Errorf("data.orig: want=kept got=%q", data["orig"])
		}
		return nil
	}, 15*time.Second)

	// The contribution inventory is persisted on the Graph.
	environment.Eventually(t, 10*time.Second, 200*time.Millisecond, func() error {
		cur := env.GetGraph(t, gKey)
		if cur.GetAnnotations()[metadata.PatchContributionsAnnotation] == "" {
			return fmt.Errorf("patch-contributions annotation not persisted")
		}
		return nil
	})

	// Remove the patch node from the spec. The controller releases the
	// contributed field on the next reconcile; the target object survives.
	env.UpdateGraphSpec(t, gKey, func(cur *expv1alpha1.Graph) {
		cur.Spec.Nodes = []expv1alpha1.Node{{
			ID:  "keep",
			Def: environment.RawExt(t, map[string]any{"x": 1}),
		}}
	})

	env.AwaitObject(t, cmGVK, cmKey, func(u *unstructured.Unstructured) error {
		data, _, _ := unstructured.NestedStringMap(u.Object, "data")
		if _, ok := data["added"]; ok {
			return fmt.Errorf("data.added still present after release")
		}
		if data["orig"] != "kept" {
			return fmt.Errorf("data.orig lost during release: got=%q", data["orig"])
		}
		return nil
	}, 15*time.Second)
}
