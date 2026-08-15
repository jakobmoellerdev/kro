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

// Package rgdadapter is the RGD-on-Graph adapter: it synthesizes and
// serves the instance CRD that a ResourceGraphDefinition currently owns
// so an RGD can later run on the Graph engine.
//
// A Graph is applied directly and has no CRD synthesis. For RGD to flip
// onto Graph, something must still produce and own the instance CRD. This
// package reuses pkg/graph + pkg/graph/crd + pkg/simpleschema + pkg/client
// + pkg/metadata — it does not reimplement synthesis or CRD
// create/update/diffing, and it does not run the instance controller or
// compose resources.
package rgdadapter

import (
	"context"
	"fmt"
	"strings"

	"github.com/gobuffalo/flect"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	kroclient "github.com/kubernetes-sigs/kro/pkg/client"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/graph/crd"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
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

// EnsureInstanceCRD synthesizes the instance CRD for rgd, stamps the kro
// ownership labels (NewKROMetaLabeler + NewResourceGraphDefinitionLabeler,
// matching ensureServingState), and applies it via crdClient.Ensure.
//
// Ownership conflicts (a CRD already owned by a different RGD, or not
// owned by kro at all) are refused by CRDClient.Ensure via
// metadata.CompareRGDOwnership — this adapter does not reimplement that
// check. On success the Established CRD is returned via crdClient.Get.
func EnsureInstanceCRD(
	ctx context.Context,
	crdClient kroclient.CRDClient,
	rgd *v1alpha1.ResourceGraphDefinition,
	allowBreakingChanges bool,
) (*extv1.CustomResourceDefinition, error) {
	if crdClient == nil {
		return nil, fmt.Errorf("crd client is required")
	}

	synthesized, err := SynthesizeInstanceCRD(rgd)
	if err != nil {
		return nil, err
	}

	labeler, err := metadata.NewKROMetaLabeler().Merge(metadata.NewResourceGraphDefinitionLabeler(rgd))
	if err != nil {
		return nil, fmt.Errorf("failed to setup CRD labeler: %w", err)
	}
	labeler.ApplyLabels(&synthesized.ObjectMeta)

	// SynthesizeInstanceCRD leaves an empty-status placeholder (no Type).
	// The apiserver rejects object fields without a type, so stamp a typed
	// empty object before serving. Default state/conditions stay deferred
	// to SetCRDStatus after Graph-engine status inference.
	crd.SetCRDStatus(synthesized, extv1.JSONSchemaProps{Type: "object"}, false)

	if err := crdClient.Ensure(ctx, *synthesized, allowBreakingChanges); err != nil {
		return nil, err
	}

	served, err := crdClient.Get(ctx, synthesized.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get served CRD %s: %w", synthesized.Name, err)
	}
	return served, nil
}

// DeleteInstanceCRD deletes the instance CRD for rgd if this RGD owns it.
// The ownership check is replicated from cleanupResourceGraphDefinitionCRD
// (that helper lives in the RGD controller package, which is too heavy to
// import from the adapter). A CRD owned by a different RGD, or not owned
// by kro, is left in place. Missing CRDs are a no-op.
func DeleteInstanceCRD(
	ctx context.Context,
	crdClient kroclient.CRDClient,
	rgd *v1alpha1.ResourceGraphDefinition,
) error {
	if crdClient == nil {
		return fmt.Errorf("crd client is required")
	}
	if rgd == nil {
		return fmt.Errorf("resourcegraphdefinition is required")
	}
	if rgd.Spec.Schema == nil {
		return fmt.Errorf("resourcegraphdefinition %q: schema is required", rgd.Name)
	}

	crdName := instanceCRDName(rgd.Spec.Schema.Group, rgd.Spec.Schema.Kind)
	existing, err := crdClient.Get(ctx, crdName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get CRD %s: %w", crdName, err)
	}

	owner, ok := existing.GetLabels()[metadata.ResourceGraphDefinitionNameLabel]
	if !ok || owner != rgd.Name {
		ctrl.LoggerFrom(ctx).V(1).Info(
			"skipping CRD deletion, not owned by this RGD",
			"crd", crdName,
			"rgd", rgd.Name,
			"owner", owner,
		)
		return nil
	}

	if err := crdClient.Delete(ctx, crdName); err != nil {
		return fmt.Errorf("failed to delete CRD %s: %w", crdName, err)
	}
	return nil
}

// instanceCRDName mirrors extractCRDName in the RGD controller cleanup
// path (pluralize(kind).group). Replicated here so the adapter does not
// import the controller package.
func instanceCRDName(group, kind string) string {
	return fmt.Sprintf("%s.%s",
		flect.Pluralize(strings.ToLower(kind)),
		group)
}
