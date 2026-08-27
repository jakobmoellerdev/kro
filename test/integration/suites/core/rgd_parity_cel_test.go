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
	"time"

	. "github.com/onsi/ginkgo/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// This file ports the CEL-feature integration scenarios (format(), omit(),
// two-variable comprehensions, metadata-field access, schema-aware Secret byte
// conversion, and arbitrary unknown nested fields) to the parity harness. CEL
// resolution in resource templates is the strongest parity signal: given the
// same fixture and instance spec, both backends must resolve every expression
// byte-identically into the same child-resource field values.
//
// Because the graph backend only writes status.ready (not RGD-authored status
// fields), scenarios whose SOURCE asserts on instance status are adapted to
// assert the same CEL result on a CHILD RESOURCE field instead, which both
// backends produce identically. Scenarios with no graph analog are marked N/A.
//
// Ported from:
//   - format_test.go                 (format())
//   - omit_test.go                   (omit())
//   - two_var_comprehensions_test.go (transformMap/transformMapEntry/transformList)
//   - metadata_fields_test.go        (schema.metadata.* access in CEL)
//   - schema_aware_cel_test.go       (Secret data bytes -> string())
//   - unknown_fields_test.go         (arbitrary unknown nested fields, optional CEL)
var _ = Describe("RGD parity (CEL features): builtin vs RGD-as-Graph", func() {
	// ── format(): "%s:%s".format([...]) into a ConfigMap data value ─────────
	//
	// Ports format_test.go. A ServiceAccount and a ConfigMap; the ConfigMap's
	// data.key is a format() over the namespace and the SA's resolved name,
	// exercising a cross-resource reference inside a CEL format() call.
	DescribeTable("format() resolves a templated string",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityFmt"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${configMap.metadata.name != ''}"},
						),
						generator.WithResource("serviceAccount", map[string]any{
							"apiVersion": "v1", "kind": "ServiceAccount",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
						}, nil, nil),
						generator.WithResource("configMap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"key": `${"%s:%s".format([schema.metadata.namespace, serviceAccount.metadata.name])}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "fmt-target"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					// Both the ServiceAccount and the ConfigMap must converge.
					awaitInstanceCR(t, schema.GroupVersionKind{Version: "v1", Kind: "ServiceAccount"},
						types.NamespacedName{Namespace: ns, Name: "fmt-target"}, nil, 60*time.Second)
					want := ns + ":fmt-target"
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "fmt-target"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "key")
							if got != want {
								return &notYetError{msg: "cm.data.key=" + got + ", want " + want}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── omit(): a ternary that yields omit() drops the field ────────────────
	//
	// Ports omit_test.go (the two convergence cases: field-omitted when the
	// optional spec value is empty, field-present when it is set). The two
	// convergence cases are split into distinct instances/RGDs, one per
	// DescribeTable, so each parity run asserts one branch across both backends.
	//
	// N/A: the source's SSA relinquish-ownership edge case (case 1c: set the
	// field, then clear it and assert SSA drops it) is an update-time SSA
	// behavior of the built-in applier's field manager; the graph backend
	// stamps per-instance Graphs and does not model the same single-manager
	// SSA relinquish semantics, so that transition is not a shared contract.
	DescribeTable("omit() drops a ConfigMap data field when its value is empty",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityOmit"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":     "string",
								"optional": `string | default=""`,
							},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"always":   "present",
								"optional": `${schema.spec.optional != "" ? schema.spec.optional : omit()}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "omit-empty", "optional": ""},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "omit-empty"},
						func(u *unstructured.Unstructured) error {
							always, _, _ := unstructured.NestedString(u.Object, "data", "always")
							if always != "present" {
								return &notYetError{msg: "cm.data.always=" + always + ", want present"}
							}
							if _, found, _ := unstructured.NestedString(u.Object, "data", "optional"); found {
								return &notYetError{msg: "cm.data.optional present, want omitted"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	DescribeTable("omit() keeps a ConfigMap data field when its value is set",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityOmitSet"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":     "string",
								"optional": `string | default=""`,
							},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"always":   "present",
								"optional": `${schema.spec.optional != "" ? schema.spec.optional : omit()}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "omit-set", "optional": "my-value"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "omit-set"},
						func(u *unstructured.Unstructured) error {
							always, _, _ := unstructured.NestedString(u.Object, "data", "always")
							optional, foundOpt, _ := unstructured.NestedString(u.Object, "data", "optional")
							if always != "present" {
								return &notYetError{msg: "cm.data.always=" + always + ", want present"}
							}
							if !foundOpt || optional != "my-value" {
								return &notYetError{msg: "cm.data.optional=" + optional + ", want my-value"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── omit() drops an array element (Pod command) ─────────────────────────
	//
	// Ports the "omit array elements" It from omit_test.go. When the optional
	// arg is empty, the ternary yields omit() and the container command list
	// collapses to ["echo"] instead of ["echo", <value>].
	DescribeTable("omit() drops an array element when its value is empty",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityOmitArr"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":        "string",
								"optionalArg": `string | default=""`,
							},
							map[string]any{"ready": "${pod.metadata.name != ''}"},
						),
						generator.WithResource("pod", map[string]any{
							"apiVersion": "v1", "kind": "Pod",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"spec": map[string]any{
								"containers": []any{
									map[string]any{
										"name":  "main",
										"image": "busybox",
										"command": []any{
											"echo",
											`${schema.spec.optionalArg != "" ? schema.spec.optionalArg : omit()}`,
										},
									},
								},
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "omit-arr-empty", "optionalArg": ""},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, schema.GroupVersionKind{Version: "v1", Kind: "Pod"},
						types.NamespacedName{Namespace: ns, Name: "omit-arr-empty"},
						func(u *unstructured.Unstructured) error {
							cmd, err := podFirstContainerCommand(u)
							if err != nil {
								return &notYetError{msg: err.Error()}
							}
							if len(cmd) != 1 || cmd[0] != "echo" {
								return &notYetError{msg: fmt.Sprintf("pod command=%v, want [echo]", cmd)}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Two-variable comprehensions (transformMap/Entry/List) ───────────────
	//
	// Ports two_var_comprehensions_test.go: four ConfigMap data values computed
	// by transformMap / transformMapEntry / transformList macros. Pure CEL, so
	// both backends must produce identical results.
	DescribeTable("two-variable comprehensions resolve identically",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityComp"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string"},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"transformMapValue":      "${string({'a': 1, 'b': 2}.transformMap(k, v, v + 10)['a'])}",
								"transformMapEntryValue": "${string(['x', 'y'].transformMapEntry(i, v, {v: string(i)})['x'])}",
								"transformListSize":      "${string({'p': 1, 'q': 2, 'r': 3}.transformList(k, v, k).size())}",
								"transformMapFiltered":   "${string([10, 20, 30].transformMap(i, v, i > 0, v * 2).size())}",
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "comp-result"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "comp-result"},
						func(u *unstructured.Unstructured) error {
							want := map[string]string{
								"transformMapValue":      "11",
								"transformMapEntryValue": "0",
								"transformListSize":      "3",
								"transformMapFiltered":   "2",
							}
							for k, v := range want {
								got, _, _ := unstructured.NestedString(u.Object, "data", k)
								if got != v {
									return &notYetError{msg: "cm.data." + k + "=" + got + ", want " + v}
								}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Metadata field access in CEL ────────────────────────────────────────
	//
	// Ports metadata_fields_test.go: the instance's metadata.* fields are read
	// via CEL into ConfigMap data values (name, namespace, uid, generation,
	// resourceVersion). Both backends publish the instance as `schema`, so all
	// fields resolve.
	DescribeTable("schema.metadata fields resolve in CEL",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityMeta"+rand.String(4), "v1alpha1",
							map[string]any{"dummy": "string"},
							map[string]any{"ready": "${testConfigMap.metadata.name != ''}"},
						),
						generator.WithResource("testConfigMap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.metadata.name}-metadata-test",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"instance-name":            "${schema.metadata.name}",
								"instance-namespace":       "${schema.metadata.namespace}",
								"instance-uid":             "${schema.metadata.uid}",
								"instance-generation":      "${string(schema.metadata.generation)}",
								"instance-resourceVersion": "${schema.metadata.resourceVersion}",
								"instance-generateName":    `${schema.metadata.?generateName.orValue("not-set")}`,
								"instance-has-labels":      `${has(schema.metadata.labels) ? "true" : "false"}`,
								"instance-has-annotations": `${has(schema.metadata.annotations) ? "true" : "false"}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{"dummy": "value"},
				func(t GinkgoTInterface, ns, instName string, _ schema.GroupVersionKind, _ rgdBackend) {
					cmName := instName + "-metadata-test"
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: cmName},
						func(u *unstructured.Unstructured) error {
							name, _, _ := unstructured.NestedString(u.Object, "data", "instance-name")
							nsGot, _, _ := unstructured.NestedString(u.Object, "data", "instance-namespace")
							uid, _, _ := unstructured.NestedString(u.Object, "data", "instance-uid")
							gen, _, _ := unstructured.NestedString(u.Object, "data", "instance-generation")
							rv, _, _ := unstructured.NestedString(u.Object, "data", "instance-resourceVersion")
							if name != instName {
								return &notYetError{msg: "instance-name=" + name + ", want " + instName}
							}
							if nsGot != ns {
								return &notYetError{msg: "instance-namespace=" + nsGot + ", want " + ns}
							}
							if uid == "" {
								return &notYetError{msg: "instance-uid empty"}
							}
							if gen == "" {
								return &notYetError{msg: "instance-generation empty"}
							}
							if rv == "" {
								return &notYetError{msg: "instance-resourceVersion empty"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Schema-aware CEL: Secret data bytes -> string() ─────────────────────
	//
	// Ports schema_aware_cel_test.go. A Secret is created via stringData (the
	// apiserver base64-encodes into data). The value read back via
	// string(secret.data.<key>) requires schema-aware conversion (data values
	// are format:"byte", so UnstructuredToVal produces cel bytes). The SOURCE
	// asserts this in the instance STATUS (status.decodedClientId); since the
	// graph backend writes only status.ready, the decoded value is asserted on
	// a CHILD ConfigMap data field instead — the identical CEL result both
	// backends must produce.
	DescribeTable("schema-aware string(secret.data) resolves the decoded value",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParitySecretCel"+rand.String(4), "v1alpha1",
							map[string]any{"name": "string", "clientId": "string"},
							map[string]any{"ready": "${decoded.metadata.name != ''}"},
						),
						generator.WithResource("secret", map[string]any{
							"apiVersion": "v1", "kind": "Secret",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"stringData": map[string]any{
								"clientId": "${schema.spec.clientId}",
							},
						}, nil, nil),
						generator.WithResource("decoded", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}-decoded",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"decodedClientId": `${string(secret.data.clientId)}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{"name": "secret-cel", "clientId": "my-secret-client"},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, secretGVK,
						types.NamespacedName{Namespace: ns, Name: "secret-cel"},
						func(u *unstructured.Unstructured) error {
							if _, found, _ := unstructured.NestedString(u.Object, "data", "clientId"); !found {
								return &notYetError{msg: "secret.data.clientId not found"}
							}
							return nil
						},
						60*time.Second,
					)
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "secret-cel-decoded"},
						func(u *unstructured.Unstructured) error {
							got, _, _ := unstructured.NestedString(u.Object, "data", "decodedClientId")
							if got != "my-secret-client" {
								return &notYetError{msg: "decodedClientId=" + got + ", want my-secret-client"}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)

	// ── Arbitrary unknown nested fields + optional CEL ──────────────────────
	//
	// Ports the shared-contract core of unknown_fields_test.go: an instance
	// carries an arbitrary nested object (schema field typed "object", which
	// synthesizes x-kubernetes-preserve-unknown-fields), and the RGD reads its
	// members via both required (schema.spec.nested.field) and optional
	// (schema.spec.nested.?field / .?absent) CEL into a ConfigMap. Both backends
	// preserve the unknown fields and resolve the optional CEL identically.
	//
	// N/A: the source's external-ref arm (a pre-created AllowUnknown CRD +
	// existing instance read via ${external.*}) and the "second RGD embeds the
	// first RGD's status schema, must not panic" arm both assert built-in
	// RGD-controller compile/status internals (external-ref schema resolution,
	// RGD status embedding), which have no standalone-Graph analog.
	DescribeTable("arbitrary unknown nested fields resolve via optional CEL",
		func(makeBackend func() rgdBackend) {
			runParityFixture(makeBackend(),
				func(name string) *krov1alpha1.ResourceGraphDefinition {
					return generator.NewResourceGraphDefinition(name,
						generator.WithSchema(
							"ParityUnknown"+rand.String(4), "v1alpha1",
							map[string]any{
								"name":   "string",
								"nested": "object",
							},
							map[string]any{"ready": "${configmap.metadata.name != ''}"},
						),
						generator.WithResource("configmap", map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{
								"name":      "${schema.spec.name}",
								"namespace": "${schema.metadata.namespace}",
							},
							"data": map[string]any{
								"name":           "${schema.spec.name}",
								"nested":         "${schema.spec.nested.field}",
								"optionalNested": `${schema.spec.nested.?field.orValue("missing")}`,
								"nonExisting":    `${schema.spec.nested.?absent.orValue("")}`,
							},
						}, nil, nil),
					)
				},
				map[string]any{
					"name":   "unknown-fields",
					"nested": map[string]any{"field": "value"},
				},
				func(t GinkgoTInterface, ns, _ string, _ schema.GroupVersionKind, _ rgdBackend) {
					awaitInstanceCR(t, configMapGVK,
						types.NamespacedName{Namespace: ns, Name: "unknown-fields"},
						func(u *unstructured.Unstructured) error {
							checks := map[string]string{
								"name":           "unknown-fields",
								"nested":         "value",
								"optionalNested": "value",
								"nonExisting":    "",
							}
							for k, want := range checks {
								got, _, _ := unstructured.NestedString(u.Object, "data", k)
								if got != want {
									return &notYetError{msg: "cm.data." + k + "=" + got + ", want " + want}
								}
							}
							return nil
						},
						60*time.Second,
					)
				},
			)
		},
		Entry("builtin", parityBuiltin),
		Entry("graph-v1alpha2", parityGraph),
	)
})

// podFirstContainerCommand extracts spec.containers[0].command from a Pod as a
// []string, tolerating the []any element shape unstructured stores. Used by the
// omit()-array scenario, where NestedStringSlice cannot index into the
// containers slice.
func podFirstContainerCommand(u *unstructured.Unstructured) ([]string, error) {
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		return nil, fmt.Errorf("spec.containers not ready")
	}
	c0, ok := containers[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("container[0] not a map")
	}
	raw, found, err := unstructured.NestedSlice(c0, "command")
	if err != nil || !found {
		return nil, fmt.Errorf("container[0].command not ready")
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("command element not a string")
		}
		out = append(out, s)
	}
	return out, nil
}
