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

import (
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"

	"github.com/kubernetes-sigs/kro/pkg/metrics"
	kwatch "github.com/kubernetes-sigs/kro/pkg/watch"
)

// WatchManager manages informer lifecycle per GVR. It is the shared
// implementation from pkg/watch; the alias keeps the dynamiccontroller import
// path stable. Unlike the graph-engine router, the dynamic controller wires a
// MetricsRecorder so informer counts and sync durations are exported.
type WatchManager = kwatch.Manager

// NewWatchManager creates a WatchManager wired to the dynamic controller's
// Prometheus metrics. The onEvent callback is invoked for every informer event
// across all GVRs.
func NewWatchManager(client metadata.Interface, resync time.Duration, onEvent EventHandler, log logr.Logger) *WatchManager {
	wm := kwatch.NewManager(client, resync, onEvent, log)
	wm.Metrics = dynMetricsRecorder{}
	return wm
}

// dynMetricsRecorder forwards WatchManager observability signals to the
// dynamic controller's Prometheus vectors in pkg/metrics.
type dynMetricsRecorder struct{}

func (dynMetricsRecorder) SetActiveWatches(active int) {
	metrics.DynWatchCount.Set(float64(active))
}

func (dynMetricsRecorder) ObserveInformerSync(gvr schema.GroupVersionResource, seconds float64) {
	metrics.DynInformerSyncDuration.WithLabelValues(gvr.String()).Observe(seconds)
}
