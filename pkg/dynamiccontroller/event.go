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

package dynamiccontroller

import kwatch "github.com/kubernetes-sigs/kro/pkg/watch"

// The event types are shared with the graph-engine watch router via
// pkg/watch. They are re-exported here as aliases so dynamiccontroller callers
// keep their existing import path while the underlying type is identical
// across both controller stacks.

// EventType identifies the kind of change that triggered an event.
type EventType = kwatch.EventType

const (
	EventAdd    = kwatch.EventAdd
	EventUpdate = kwatch.EventUpdate
	EventDelete = kwatch.EventDelete
)

// Event is a normalized watch event emitted by the WatchManager.
type Event = kwatch.Event

// EventHandler processes a single watch event.
type EventHandler = kwatch.EventHandler
