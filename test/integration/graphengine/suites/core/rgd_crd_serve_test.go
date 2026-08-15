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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	kroclient "github.com/kubernetes-sigs/kro/pkg/client"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/graphengine/environment"
)

// TestRGDCRDServeCreate applies a fresh RGD via EnsureInstanceCRD and
// checks the served CRD's GVK / plural / scope / spec fields plus kro
// ownership labels. Mirrors crd_test.go "should create CRD when
// ResourceGraphDefinition is created".
func TestRGDCRDServeCreate(t *testing.T) {
	env := environment.Shared(t)
	crdClient := newCRDClient(t, env)

	rgd := newServeRGD("test-crd-create", "ServeCreate", map[string]interface{}{
		"field1": "string",
		"field2": "integer | default=42",
	})
	cleanupInstanceCRD(t, crdClient, rgd)

	crd, err := rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd, false)
	if err != nil {
		t.Fatalf("EnsureInstanceCRD: %v", err)
	}

	if crd.Name != "servecreates.kro.run" {
		t.Fatalf("crd name = %q, want servecreates.kro.run", crd.Name)
	}
	if crd.Spec.Group != "kro.run" {
		t.Fatalf("group = %q, want kro.run", crd.Spec.Group)
	}
	if crd.Spec.Names.Kind != "ServeCreate" {
		t.Fatalf("kind = %q, want ServeCreate", crd.Spec.Names.Kind)
	}
	if crd.Spec.Names.Plural != "servecreates" {
		t.Fatalf("plural = %q, want servecreates", crd.Spec.Names.Plural)
	}
	if crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
		t.Fatalf("scope = %q, want Namespaced", crd.Spec.Scope)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != "v1alpha1" {
		t.Fatalf("versions = %+v, want [v1alpha1]", crd.Spec.Versions)
	}

	props := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties
	if props["spec"].Properties["field1"].Type != "string" {
		t.Fatalf("field1 type = %q, want string", props["spec"].Properties["field1"].Type)
	}
	if props["spec"].Properties["field2"].Type != "integer" {
		t.Fatalf("field2 type = %q, want integer", props["spec"].Properties["field2"].Type)
	}
	if string(props["spec"].Properties["field2"].Default.Raw) != "42" {
		t.Fatalf("field2 default = %s, want 42", props["spec"].Properties["field2"].Default.Raw)
	}

	if !metadata.IsKROOwned(&crd.ObjectMeta) {
		t.Fatalf("expected kro ownership labels on served CRD")
	}
	if got := crd.Labels[metadata.ResourceGraphDefinitionNameLabel]; got != rgd.Name {
		t.Fatalf("rgd name label = %q, want %q", got, rgd.Name)
	}
	if got := crd.Labels[metadata.ResourceGraphDefinitionIDLabel]; got != string(rgd.UID) {
		t.Fatalf("rgd id label = %q, want %q", got, rgd.UID)
	}
}

// TestRGDCRDServeNonBreakingUpdate adds an optional field and rewrites
// shortNames / categories, then re-ensures. Mirrors crd_test.go
// non-breaking schema + names updates.
func TestRGDCRDServeNonBreakingUpdate(t *testing.T) {
	env := environment.Shared(t)
	crdClient := newCRDClient(t, env)

	rgd := newServeRGD("test-crd-update", "ServeUpdate", map[string]interface{}{
		"field1": "string",
	})
	rgd.Spec.Schema.ShortNames = []string{"su", "supdate"}
	rgd.Spec.Schema.Categories = []string{"kro", "platform"}
	cleanupInstanceCRD(t, crdClient, rgd)

	crd, err := rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd, false)
	if err != nil {
		t.Fatalf("initial EnsureInstanceCRD: %v", err)
	}
	if !equalStrings(crd.Spec.Names.ShortNames, []string{"su", "supdate"}) {
		t.Fatalf("shortNames = %v, want [su supdate]", crd.Spec.Names.ShortNames)
	}
	if !equalStrings(crd.Spec.Names.Categories, []string{"kro", "platform"}) {
		t.Fatalf("categories = %v, want [kro platform]", crd.Spec.Names.Categories)
	}

	setServeSpec(rgd, map[string]interface{}{
		"field1": "string",
		"field2": "integer | default=42",
	})
	rgd.Spec.Schema.ShortNames = []string{"su2"}
	rgd.Spec.Schema.Categories = []string{"platform"}

	crd, err = rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd, false)
	if err != nil {
		t.Fatalf("non-breaking EnsureInstanceCRD: %v", err)
	}

	props := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties
	if len(props["spec"].Properties) != 2 {
		t.Fatalf("spec properties = %d, want 2", len(props["spec"].Properties))
	}
	if props["spec"].Properties["field2"].Type != "integer" {
		t.Fatalf("field2 type = %q, want integer", props["spec"].Properties["field2"].Type)
	}
	if !equalStrings(crd.Spec.Names.ShortNames, []string{"su2"}) {
		t.Fatalf("updated shortNames = %v, want [su2]", crd.Spec.Names.ShortNames)
	}
	if !equalStrings(crd.Spec.Names.Categories, []string{"platform"}) {
		t.Fatalf("updated categories = %v, want [platform]", crd.Spec.Names.Categories)
	}
}

// TestRGDCRDServeDeleteRecreate deletes the served CRD and ensures it
// comes back. Mirrors crd_test.go delete + recreate-on-delete.
func TestRGDCRDServeDeleteRecreate(t *testing.T) {
	env := environment.Shared(t)
	crdClient := newCRDClient(t, env)

	rgd := newServeRGD("test-crd-recreate", "ServeRecreate", map[string]interface{}{
		"field1": "string",
	})
	cleanupInstanceCRD(t, crdClient, rgd)

	crd, err := rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd, false)
	if err != nil {
		t.Fatalf("initial EnsureInstanceCRD: %v", err)
	}
	originalUID := crd.UID
	crdName := crd.Name

	if err := rgdadapter.DeleteInstanceCRD(env.Ctx, crdClient, rgd); err != nil {
		t.Fatalf("DeleteInstanceCRD: %v", err)
	}
	awaitCRDGone(t, env, crdClient, crdName)

	recreated, err := rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd, false)
	if err != nil {
		t.Fatalf("recreate EnsureInstanceCRD: %v", err)
	}
	if recreated.UID == originalUID {
		t.Fatalf("recreated CRD kept original UID %s", originalUID)
	}
	if recreated.Name != crdName {
		t.Fatalf("recreated name = %q, want %q", recreated.Name, crdName)
	}
	if !metadata.IsKROOwned(&recreated.ObjectMeta) {
		t.Fatalf("expected kro ownership labels on recreated CRD")
	}
	if got := recreated.Labels[metadata.ResourceGraphDefinitionNameLabel]; got != rgd.Name {
		t.Fatalf("rgd name label = %q, want %q", got, rgd.Name)
	}
	props := recreated.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties
	if props["spec"].Properties["field1"].Type != "string" {
		t.Fatalf("recreated field1 type = %q, want string", props["spec"].Properties["field1"].Type)
	}
}

// TestRGDCRDServeOwnership refuses to clobber a CRD owned by a
// different RGD. Ownership conflict lives in CRDClient.Ensure
// (metadata.CompareRGDOwnership); the adapter just stamps labels and
// delegates. Mirrors crd_test.go "should prevent multiple
// ResourceGraphDefinitions from managing the same CRD".
func TestRGDCRDServeOwnership(t *testing.T) {
	env := environment.Shared(t)
	crdClient := newCRDClient(t, env)

	rgd1 := newServeRGD("test-crd-owner-1", "ServeOwner", map[string]interface{}{
		"field1": "string",
	})
	cleanupInstanceCRD(t, crdClient, rgd1)

	crd, err := rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd1, false)
	if err != nil {
		t.Fatalf("rgd1 EnsureInstanceCRD: %v", err)
	}
	if crd.Labels[metadata.ResourceGraphDefinitionNameLabel] != rgd1.Name {
		t.Fatalf("owner label = %q, want %q", crd.Labels[metadata.ResourceGraphDefinitionNameLabel], rgd1.Name)
	}

	rgd2 := newServeRGD("test-crd-owner-2", "ServeOwner", map[string]interface{}{
		"field1": "string",
		"field2": "integer",
	})

	_, err = rgdadapter.EnsureInstanceCRD(env.Ctx, crdClient, rgd2, false)
	if err == nil {
		t.Fatal("expected ownership conflict from rgd2 EnsureInstanceCRD")
	}
	if !strings.Contains(err.Error(), "owned by another ResourceGraphDefinition") {
		t.Fatalf("ownership error = %q, want substring %q", err.Error(), "owned by another ResourceGraphDefinition")
	}

	still, err := crdClient.Get(env.Ctx, crd.Name)
	if err != nil {
		t.Fatalf("get CRD after refused ensure: %v", err)
	}
	if still.Labels[metadata.ResourceGraphDefinitionNameLabel] != rgd1.Name {
		t.Fatalf("owner clobbered to %q", still.Labels[metadata.ResourceGraphDefinitionNameLabel])
	}
	if _, ok := still.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["field2"]; ok {
		t.Fatal("rgd2 schema was applied despite ownership conflict")
	}

	// Delete from the non-owner must leave the CRD in place (replicated
	// cleanupResourceGraphDefinitionCRD check).
	if err := rgdadapter.DeleteInstanceCRD(env.Ctx, crdClient, rgd2); err != nil {
		t.Fatalf("DeleteInstanceCRD from non-owner: %v", err)
	}
	if _, err := crdClient.Get(env.Ctx, crd.Name); err != nil {
		t.Fatalf("non-owner delete removed the CRD: %v", err)
	}
}

func newCRDClient(t *testing.T, env *environment.Env) kroclient.CRDClient {
	t.Helper()
	clientSet, err := kroclient.NewSet(kroclient.Config{RestConfig: env.Cfg})
	if err != nil {
		t.Fatalf("new client set: %v", err)
	}
	return clientSet.CRD(kroclient.CRDWrapperConfig{})
}

func newServeRGD(name, kind string, spec map[string]interface{}) *krov1alpha1.ResourceGraphDefinition {
	rgd := generator.NewResourceGraphDefinition(name,
		generator.WithSchema(kind, "v1alpha1", spec, nil),
	)
	rgd.Spec.Schema.Group = "kro.run"
	rgd.UID = types.UID(name + "-uid")
	return rgd
}

func setServeSpec(rgd *krov1alpha1.ResourceGraphDefinition, spec map[string]interface{}) {
	raw, err := json.Marshal(spec)
	if err != nil {
		panic(err)
	}
	rgd.Spec.Schema.Spec = runtime.RawExtension{
		Object: &unstructured.Unstructured{Object: spec},
		Raw:    raw,
	}
}

func cleanupInstanceCRD(t *testing.T, crdClient kroclient.CRDClient, rgd *krov1alpha1.ResourceGraphDefinition) {
	t.Helper()
	t.Cleanup(func() {
		_ = rgdadapter.DeleteInstanceCRD(context.Background(), crdClient, rgd)
	})
}

func awaitCRDGone(t *testing.T, env *environment.Env, crdClient kroclient.CRDClient, name string) {
	t.Helper()
	environment.Eventually(t, 30*time.Second, time.Second, func() error {
		_, err := crdClient.Get(env.Ctx, name)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("crd %s still present", name)
	})
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
