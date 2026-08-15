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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph/hash"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

// newRevisionTestCompiler builds a Compiler backed by fake discovery — the
// same pattern used by reconcile_parity_test.go.
func newRevisionTestCompiler(t *testing.T) *compiler.Compiler {
	t.Helper()
	fakeResolver, disco := testk8s.NewFakeResolver()
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	return compiler.NewCompilerWithDependencies(fakeResolver, rm)
}

// goodRGD returns a minimal RGD with one ConfigMap template.  It translates
// and compiles successfully against the fake resolver.
func goodRGD() *v1alpha1.ResourceGraphDefinition {
	return &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "webapp", Namespace: "default"},
		Spec: v1alpha1.ResourceGraphDefinitionSpec{
			Resources: []*v1alpha1.Resource{
				{
					ID: "cm1",
					Template: runtime.RawExtension{Raw: []byte(`{
						"apiVersion": "v1",
						"kind": "ConfigMap",
						"metadata": {"name": "cm1", "namespace": "default"},
						"data": {"key": "value"}
					}`)},
				},
			},
		},
	}
}

// invalidRGD returns an RGD whose template references a non-existent node ID
// ("ghost"), which causes a compile error ("unknown identifier").
func invalidRGD() *v1alpha1.ResourceGraphDefinition {
	return &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "webapp", Namespace: "default"},
		Spec: v1alpha1.ResourceGraphDefinitionSpec{
			Resources: []*v1alpha1.Resource{
				{
					ID: "cm1",
					Template: runtime.RawExtension{Raw: []byte(`{
						"apiVersion": "v1",
						"kind": "ConfigMap",
						"metadata": {"name": "cm1", "namespace": "default"},
						"data": {"key": "${ghost.metadata.name}"}
					}`)},
				},
			},
		},
	}
}

// TestRevision_GoodRGDCompiles verifies that a valid RGD is translated,
// compiled, stored in the registry, and returned with degraded=false.
func TestRevision_GoodRGDCompiles(t *testing.T) {
	reg := registry.New()
	key := types.NamespacedName{Name: "webapp", Namespace: "default"}
	c := newRevisionTestCompiler(t)
	rgd := goodRGD()

	prog, servedHash, degraded, err := ResolveProgram(reg, key, rgd, c)

	require.NoError(t, err)
	assert.NotNil(t, prog, "program must be non-nil on success")
	assert.False(t, degraded, "healthy compile must not be degraded")

	// Verify the served hash matches the RGD spec fingerprint.
	expectedHash, hashErr := hash.Spec(rgd.Spec)
	require.NoError(t, hashErr)
	assert.Equal(t, expectedHash, servedHash, "served hash must equal spec hash")

	// Verify the program is stored in the registry.
	cached, hit := reg.Lookup(key, expectedHash)
	assert.True(t, hit, "compiled program must be stored in registry")
	assert.Equal(t, prog, cached)
}

// TestRevision_SameRGDServedFromCache verifies that calling ResolveProgram a
// second time with an identical spec returns the cached program (no
// recompilation) and is still non-degraded.
func TestRevision_SameRGDServedFromCache(t *testing.T) {
	reg := registry.New()
	key := types.NamespacedName{Name: "webapp", Namespace: "default"}
	c := newRevisionTestCompiler(t)
	rgd := goodRGD()

	// First call — compiles and stores.
	prog1, hash1, _, err := ResolveProgram(reg, key, rgd, c)
	require.NoError(t, err)

	// Second call — must return the same program from cache.
	prog2, hash2, degraded, err := ResolveProgram(reg, key, rgd, c)

	require.NoError(t, err)
	assert.False(t, degraded, "cache hit must not be degraded")
	assert.Equal(t, hash1, hash2, "hash must be stable across calls")
	assert.Equal(t, prog1, prog2, "same pointer returned from cache")
}

// TestRevision_InvalidSpecFallsBackToLastGood verifies the core last-good-config
// guarantee: after one successful compile, an updated RGD that fails to compile
// returns the PREVIOUS good program with degraded=true and a non-nil error.
// The caller can use this to mark the RGD not-ready while keeping instances alive.
func TestRevision_InvalidSpecFallsBackToLastGood(t *testing.T) {
	reg := registry.New()
	key := types.NamespacedName{Name: "webapp", Namespace: "default"}
	c := newRevisionTestCompiler(t)

	// Step 1: establish a good compile.
	good := goodRGD()
	goodProg, goodHash, degraded, err := ResolveProgram(reg, key, good, c)
	require.NoError(t, err)
	require.False(t, degraded)
	require.NotNil(t, goodProg)

	// Step 2: present an invalid spec (references non-existent node "ghost").
	bad := invalidRGD()
	prog, servedHash, degraded, err := ResolveProgram(reg, key, bad, c)

	// Must return an error describing the compile failure.
	require.Error(t, err, "invalid spec must return an error")
	// Must be degraded (falling back to last good).
	assert.True(t, degraded, "must be degraded when falling back to last good")
	// Must serve the LAST GOOD program, not nil.
	assert.NotNil(t, prog, "last-good program must be non-nil when a prior exists")
	// The served hash must be the good spec's hash, not the bad spec's hash.
	assert.Equal(t, goodHash, servedHash, "served hash must be the last-good hash")
	// The last-good program pointer must be the one from the first compile.
	assert.Equal(t, goodProg, prog, "served program must be the last-good program")
}

// TestRevision_FirstCompileFailure verifies that when the very first compile
// for a key fails (no prior good program), ResolveProgram returns prog=nil and
// an error. There is nothing to fall back to.
func TestRevision_FirstCompileFailure(t *testing.T) {
	reg := registry.New()
	// Use a fresh key — nothing has ever been stored for it.
	key := types.NamespacedName{Name: "brand-new", Namespace: "default"}
	c := newRevisionTestCompiler(t)

	// invalidRGD references "ghost" which doesn't exist — compile fails.
	bad := invalidRGD()
	// Fix the name so it doesn't collide with other keys.
	bad.Name = "brand-new"

	prog, servedHash, degraded, err := ResolveProgram(reg, key, bad, c)

	require.Error(t, err, "first-ever compile failure must return an error")
	assert.Nil(t, prog, "prog must be nil when no prior good program exists")
	assert.Empty(t, servedHash, "servedHash must be empty when no prior exists")
	assert.False(t, degraded, "degraded is false when there is no fallback at all")
}
