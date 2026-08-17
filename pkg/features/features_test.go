// Copyright 2025 The Kubernetes Authors.
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

package features

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefaultFeatureGatesAreDisabled verifies that all Alpha/Beta features
// are disabled by default, as per the Kubernetes feature gate convention.
func TestDefaultFeatureGatesAreDisabled(t *testing.T) {
	assert.False(t, FeatureGate.Enabled(InstanceConditionEvents),
		"InstanceConditionEvents should be disabled by default (Alpha)")
	assert.False(t, FeatureGate.Enabled(InstanceConditionMetrics),
		"InstanceConditionMetrics should be disabled by default (Alpha)")
	assert.False(t, FeatureGate.Enabled(CELOmitFunction),
		"CELOmitFunction should be disabled by default (Alpha)")
}

// TestEnableFeatureViaSet verifies that a feature can be enabled by calling
// FeatureGate.Set() with a "key=value" string, mimicking the --feature-gates flag.
func TestEnableFeatureViaSet(t *testing.T) {
	// Use a deep copy so we don't mutate the global FeatureGate between tests.
	gate := FeatureGate.DeepCopy()
	require.NoError(t, gate.Set("InstanceConditionEvents=true"))
	assert.True(t, gate.Enabled(InstanceConditionEvents))
}

// TestDisableFeatureViaSet verifies that a feature can be
// explicitly disabled via Set() after being enabled.
func TestDisableFeatureViaSet(t *testing.T) {
	gate := FeatureGate.DeepCopy()
	require.NoError(t, gate.Set("InstanceConditionEvents=true"))
	assert.True(t, gate.Enabled(InstanceConditionEvents))

	require.NoError(t, gate.Set("InstanceConditionEvents=false"))
	assert.False(t, gate.Enabled(InstanceConditionEvents))

	require.NoError(t, gate.Set("InstanceConditionMetrics=true"))
	assert.True(t, gate.Enabled(InstanceConditionMetrics))

	require.NoError(t, gate.Set("InstanceConditionMetrics=false"))
	assert.False(t, gate.Enabled(InstanceConditionMetrics))
}

// TestSetUnknownFeatureReturnsError verifies that specifying an unknown
// feature gate name returns an error rather than silently succeeding.
func TestSetUnknownFeatureReturnsError(t *testing.T) {
	gate := FeatureGate.DeepCopy()
	err := gate.Set("NonExistentFeature=true")
	require.Error(t, err, "setting an unknown feature should return an error")
}

// TestDeferUnresolvedSchema verifies the policy point that decides whether an
// unresolvable resource is deferred: it requires BOTH the
// DeferredSchemaResolution gate to be enabled AND a non-empty includeWhen.
func TestDeferUnresolvedSchema(t *testing.T) {
	tests := []struct {
		name           string
		gateEnabled    bool
		hasIncludeWhen bool
		want           bool
	}{
		{
			name:           "gate off, includeWhen present",
			gateEnabled:    false,
			hasIncludeWhen: true,
			want:           false,
		},
		{
			name:           "gate off, no includeWhen",
			gateEnabled:    false,
			hasIncludeWhen: false,
			want:           false,
		},
		{
			name:           "gate on, no includeWhen",
			gateEnabled:    true,
			hasIncludeWhen: false,
			want:           false,
		},
		{
			name:           "gate on, includeWhen present",
			gateEnabled:    true,
			hasIncludeWhen: true,
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Toggle the global gate and restore it afterwards. This test
			// mutates the shared FeatureGate, so it must not run in parallel.
			if tt.gateEnabled {
				require.NoError(t, FeatureGate.Set("DeferredSchemaResolution=true"))
			} else {
				require.NoError(t, FeatureGate.Set("DeferredSchemaResolution=false"))
			}
			t.Cleanup(func() {
				require.NoError(t, FeatureGate.Set("DeferredSchemaResolution=false"))
			})

			assert.Equal(t, tt.want, DeferUnresolvedSchema(tt.hasIncludeWhen))
		})
	}
}

// TestKnownFeaturesContainsAllRegistered verifies that KnownFeatures() lists
// all features that were registered in defaultKroFeatureGates.
func TestKnownFeaturesContainsAllRegistered(t *testing.T) {
	known := FeatureGate.KnownFeatures()
	knownStr := strings.Join(known, " ")

	assert.Contains(t, knownStr, string(InstanceConditionEvents))
	assert.Contains(t, knownStr, string(InstanceConditionMetrics))
	assert.Contains(t, knownStr, string(CELOmitFunction))
}
