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
	"fmt"

	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph/hash"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
)

// ResolveProgram is the last-good-config entrypoint for the RGD-on-Graph path.
//
// It maps an RGD spec revision onto a compiled Program using the following
// logic:
//
//  1. Compute hash.Spec(rgd.Spec) — the normalized fingerprint of the spec.
//  2. If the registry already has a program for (key, hash), return it
//     immediately (cache hit, degraded=false).
//  3. Otherwise translate (ResourceGraphDefinitionToGraph) and compile.
//     On success: Store in the registry and return (degraded=false).
//  4. On compile failure:
//     a. If a prior program exists for key (any hash — the last good revision),
//        return THAT program with degraded=true and a non-nil err so the
//        caller can mark the RGD not-ready while keeping instances running.
//     b. If no prior program exists (first compile ever failed), return
//        prog=nil and the compile error so the caller can mark the RGD
//        not-ready without serving anything.
//
// The returned servedHash is the hash of the program actually being served
// (may differ from the current spec's hash when degraded=true).
func ResolveProgram(
	reg *registry.Registry,
	key types.NamespacedName,
	rgd *v1alpha1.ResourceGraphDefinition,
	c Compiler,
) (prog *compiler.Program, servedHash string, degraded bool, err error) {
	if reg == nil {
		return nil, "", false, fmt.Errorf("rgdadapter: registry is required")
	}
	if rgd == nil {
		return nil, "", false, fmt.Errorf("rgdadapter: rgd is required")
	}
	if c == nil {
		return nil, "", false, fmt.Errorf("rgdadapter: compiler is required")
	}

	// Step 1: fingerprint the current spec.
	currentHash, err := hash.Spec(rgd.Spec)
	if err != nil {
		return nil, "", false, fmt.Errorf("rgdadapter: hash spec: %w", err)
	}

	// Step 2: cache hit — spec unchanged since last successful compile.
	if cached, hit := reg.Lookup(key, currentHash); hit {
		return cached, currentHash, false, nil
	}

	// Step 3: translate + compile.
	g, translateErr := ResourceGraphDefinitionToGraph(rgd)
	if translateErr != nil {
		// Translation failure is treated as a compile failure for the
		// last-good-config fallback.
		return resolveWithFallback(reg, key, fmt.Errorf("rgdadapter: translate: %w", translateErr))
	}

	compiled, compileErr := c.Compile(g)
	if compileErr != nil {
		return resolveWithFallback(reg, key, fmt.Errorf("rgdadapter: compile: %w", compileErr))
	}

	// Success: promote to cache and return the fresh program.
	reg.Store(key, currentHash, compiled)
	return compiled, currentHash, false, nil
}

// resolveWithFallback implements the last-good-config branch: when the current
// spec fails to compile, return the previously-stored program (if any) with
// degraded=true and the original compile error preserved.
func resolveWithFallback(
	reg *registry.Registry,
	key types.NamespacedName,
	compileErr error,
) (prog *compiler.Program, servedHash string, degraded bool, err error) {
	prior, priorHash, hasPrior := reg.LastGood(key)
	if !hasPrior {
		// No prior good program — first compile ever failed.  The caller
		// cannot serve anything; return the error unmodified.
		return nil, "", false, compileErr
	}
	// Serve the last good program in degraded mode.
	return prior, priorHash, true, compileErr
}
