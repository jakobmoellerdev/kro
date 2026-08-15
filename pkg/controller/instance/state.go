// Copyright 2025 The Kube Resource Orchestrator Authors
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

package instance

import (
	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// StateManager tracks the instance-level reconciliation state. The deletion
// path sets State directly; initialStatus reads it to project .status.state.
type StateManager struct {
	State v1alpha1.InstanceState
}

// newStateManager constructs a StateManager initialized to InProgress.
func newStateManager() *StateManager {
	return &StateManager{
		State: v1alpha1.InstanceStateInProgress,
	}
}
