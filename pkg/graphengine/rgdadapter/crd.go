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

// Package rgdadapter is the first increment of the RGD-on-Graph adapter:
// it synthesizes the instance CRD that a ResourceGraphDefinition currently
// owns so an RGD can later run on the Graph engine.
//
// A Graph is applied directly and has no CRD synthesis. For RGD to flip
// onto Graph, something must still produce and own the instance CRD. This
// package reuses pkg/graph + pkg/graph/crd + pkg/simpleschema — it does
// not reimplement synthesis, and it does not run the instance controller
// or compose resources.
package rgdadapter

import (
	"fmt"

	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graph/crd"
)

// SynthesizeInstanceCRD builds the instance CRD for rgd from Spec.Schema
// (SimpleSchema → OpenAPI, then crd.SynthesizeCRD) with an empty-status
// placeholder. Status inference and default status fields are left to a
// later increment — matching NewResourceGraphDefinition's pre-SetCRDStatus
// CRD. Scope is taken from rgd.Spec.Schema.Scope.
func SynthesizeInstanceCRD(rgd *v1alpha1.ResourceGraphDefinition) (*extv1.CustomResourceDefinition, error) {
	if rgd == nil {
		return nil, fmt.Errorf("resourcegraphdefinition is required")
	}
	if rgd.Spec.Schema == nil {
		return nil, fmt.Errorf("resourcegraphdefinition %q: schema is required", rgd.Name)
	}

	rgSchema := rgd.Spec.Schema
	instanceSpecSchema, err := graph.BuildInstanceSpecSchema(rgSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to build instance spec schema: %w", err)
	}

	scope := extv1.NamespaceScoped
	if rgSchema.Scope == v1alpha1.ResourceScopeCluster {
		scope = extv1.ClusterScoped
	}

	return crd.SynthesizeCRD(
		rgSchema.Group,
		rgSchema.APIVersion,
		rgSchema.Kind,
		*instanceSpecSchema,
		extv1.JSONSchemaProps{}, // empty status placeholder
		false,                   // don't add default status fields yet
		scope,
		rgSchema,
	), nil
}
