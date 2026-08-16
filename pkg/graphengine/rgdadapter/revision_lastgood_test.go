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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
)

func resolveKey() types.NamespacedName {
	return types.NamespacedName{Name: "webapp"}
}

// ResolveProgram is the last-good-config path. An RGD whose new spec fails to
// compile must keep serving the previously compiled program so instances that
// are already running do not break, while the RGD itself is marked not-ready.
// The three outcomes are distinguished by (prog, degraded, err), and conflating
// any two of them either breaks running instances or hides a broken spec.
func TestResolveProgram_Guards(t *testing.T) {
	t.Parallel()

	rgd := testRGD(nil)
	stub := &stubCompiler{prog: emptyProgram()}

	_, _, _, err := ResolveProgram(nil, resolveKey(), rgd, stub)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "registry is required")

	_, _, _, err = ResolveProgram(registry.New(), resolveKey(), nil, stub)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rgd is required")

	_, _, _, err = ResolveProgram(registry.New(), resolveKey(), rgd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compiler is required")
}

// A spec that has not changed since the last successful compile must not be
// recompiled: compilation is the expensive part, and the cache is keyed by the
// spec fingerprint so an unchanged spec is a guaranteed hit.
func TestResolveProgram_UnchangedSpecIsServedFromCache(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	rgd := testRGD(nil)
	stub := &stubCompiler{prog: emptyProgram()}

	first, hash1, degraded, err := ResolveProgram(reg, resolveKey(), rgd, stub)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.False(t, degraded)
	assert.Equal(t, 1, stub.calls, "the first resolve must compile")

	second, hash2, degraded, err := ResolveProgram(reg, resolveKey(), rgd, stub)
	require.NoError(t, err)
	assert.False(t, degraded)
	assert.Equal(t, hash1, hash2, "an unchanged spec keeps its fingerprint")
	assert.Same(t, first, second, "the cached program must be returned verbatim")
	assert.Equal(t, 1, stub.calls, "an unchanged spec must not recompile")
}

// The load-bearing case: a new spec that fails to compile falls back to the
// last good program with degraded=true AND a non-nil error, so the caller can
// mark the RGD not-ready while instances keep reconciling against the program
// that still works.
func TestResolveProgram_CompileFailureServesLastGoodDegraded(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	good := testRGD(nil)
	stub := &stubCompiler{prog: emptyProgram()}

	lastGood, goodHash, _, err := ResolveProgram(reg, resolveKey(), good, stub)
	require.NoError(t, err)
	require.NotNil(t, lastGood)

	// A different spec (so the hash misses the cache) that fails to compile.
	broken := testRGD(nil)
	broken.Spec.Resources = append(broken.Spec.Resources, &v1alpha1.Resource{
		ID:       "second",
		Template: rawResource(map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}),
	})
	stub.err = errors.New("undeclared reference to 'missing'")

	served, servedHash, degraded, err := ResolveProgram(reg, resolveKey(), broken, stub)

	require.Error(t, err, "the caller must learn the new spec is broken")
	assert.Contains(t, err.Error(), "compile")
	assert.Contains(t, err.Error(), "undeclared reference",
		"the compiler's message must survive so it can reach the RGD condition")
	assert.True(t, degraded, "serving a stale program must be flagged as degraded")
	assert.Same(t, lastGood, served,
		"the previously compiled program must keep serving running instances")
	assert.Equal(t, goodHash, servedHash,
		"servedHash must describe what is actually being served, not the broken spec")
}

// With no prior program there is nothing to fall back to, so the caller gets
// no program at all rather than something arbitrary to serve.
func TestResolveProgram_FirstCompileFailureServesNothing(t *testing.T) {
	t.Parallel()

	stub := &stubCompiler{err: errors.New("type mismatch")}

	prog, _, degraded, err := ResolveProgram(registry.New(), resolveKey(), testRGD(nil), stub)

	require.Error(t, err)
	assert.Nil(t, prog, "a first-ever compile failure must not serve a program")
	assert.False(t, degraded,
		"degraded means 'serving something stale'; there is nothing to serve here")
}

// A translation failure takes the same last-good path as a compile failure: an
// RGD shape the adapter cannot map is just as broken as one that will not
// compile, and running instances should be equally protected.
func TestResolveProgram_TranslationFailureUsesTheSameFallback(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	stub := &stubCompiler{prog: emptyProgram()}

	lastGood, _, _, err := ResolveProgram(reg, resolveKey(), testRGD(nil), stub)
	require.NoError(t, err)

	// Neither template nor externalRef: rejected during translation.
	broken := &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "webapp"},
		Spec: v1alpha1.ResourceGraphDefinitionSpec{
			Resources: []*v1alpha1.Resource{{ID: "broken"}},
		},
	}

	served, _, degraded, err := ResolveProgram(reg, resolveKey(), broken, stub)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "translate")
	assert.True(t, errors.Is(err, ErrUnsupported),
		"the underlying ErrUnsupported must survive the fallback wrapping")
	assert.True(t, degraded)
	assert.Same(t, lastGood, served)
}
