### annotation_label_test.go

- [ ] should have correct conditions when ResourceGraphDefinition is created

### applyset_test.go

- [ ] should apply resources with correct KEP applyset labels and annotations
- [ ] should prune resources when includeWhen becomes false
- [ ] should resolve includeWhen from upstream resources and prune when they flip
- [ ] should prune downstream dependents when a resource-backed includeWhen flips false
- [ ] should prune status-backed includeWhen dependents when upstream status flips false
- [ ] should grow annotations when resources are added and shrink when removed
- [ ] should apply correct collection-specific labels to forEach resources
- [ ] should prune collection items when collection shrinks
- [ ] should not prune resources from different RGDs
- [ ] should not prune resources from different instances of the same RGD
- [ ] should preserve user-defined labels and annotations on instance after reconciliation

### cascading_deletion_test.go

- [ ] should cascade delete all nested RGD instances and their resources

### collection_behavior_test.go

- [ ] enforces the configured collection size limit
- [ ] corrects drift on collection items in every namespace the collection spans

### collection_test.go

- [ ] should create multiple ConfigMaps from a forEach collection
- [ ] should handle cartesian product with multiple forEach iterators
- [ ] should create collection with includeWhen condition
- [ ] should toggle collection resources when includeWhen changes
- [ ] should create collection with dependency on regular resource
- [ ] should handle collection chaining with dynamic forEach expression
- [ ] should handle collection-to-collection chaining
- [ ] should handle empty collection list gracefully
- [ ] should handle deep chaining with scale up and scale down
- [ ] should block dependent resources until collection readyWhen is satisfied
- [ ] should evaluate readyWhen per-item expressions with each keyword
- [ ] should create collection resources without readyWhen expressions
- [ ] should delete all collection resources when instance is deleted
- [ ] should detect and restore drift in collection resources
- [ ] should keep instance IN_PROGRESS until collection readyWhen is satisfied (no dependents)
- [ ] should delete collection resources across multiple namespaces
- [ ] should allow dependents to reference empty collections in CEL expressions

### collection_watch_test.go

- [ ] should reactively reconcile when a non-last collection item is modified
- [ ] should not react to externally created resources after collection shrinks

### crd_test.go

- [ ] should create CRD when ResourceGraphDefinition is created
- [ ] should update CRD when ResourceGraphDefinition is updated
- [ ] should update CRD short names and categories
- [ ] should delete CRD when ResourceGraphDefinition is deleted
- [ ] should prevent multiple ResourceGraphDefinitions from managing the same CRD
- [ ] should not disrupt first RGD's controller when deleting a conflicting RGD
- [ ] should block RGD update when schema has breaking changes
- [ ] should allow RGD update when schema changes are non-breaking
- [ ] should update CRD when minimum/maximum markers are removed
- [ ] should reconcile the ResourceGraphDefinition back when CRD is manually modified
- [ ] should detect externally deleted CRD and recreate it

### data_pending_test.go

- [ ] should surface data pending condition when resources depend on unavailable status fields
- [ ] should create independent resources in parallel while waiting for dependent data

### dependency_readiness_test.go

- [ ] should wait for all dependencies to be ready before creating dependent resource
- [ ] should block dependent resources until readyWhen conditions are satisfied

### externalref_deletion_test.go

- [ ] should delete managed resources before removing finalizer when external ref is topological root
- [ ] ordered deletion waits for the higher dependency wave

### externalref_test.go

- [ ] should handle ResourceGraphDefinition with ExternalRef
- [ ] should handle ExternalRef to CRD with CEL expressions in metadata
- [ ] should list all resources when external collection has empty selector
- [ ] should list external collection resources across all namespaces when namespace is omitted

### externalref_watch_test.go

- [ ] external ref watch triggers re-reconciliation on change
- [ ] external collection with matchExpressions CEL resolves dynamic values
- [ ] external collection watch reacts to new matching resources
- [ ] external collection sortBy orders resources by a data field
- [ ] external ref to a chained RGD keeps the producer instance reconciling after consumer deletion

### format_test.go

- [ ] should resolve formatted strings using format()

### graphrevision_test.go

- [ ] should keep serving the last good revision when a later RGD update is invalid
- [ ] should resolve the compiled graph from the registry and reconcile instances
- [ ] should serve updated graph to instances after RGD spec change
- [ ] should reject updates to an existing GraphRevision spec
- [ ] should serve instances using compiled graph from current revision

### include_when_test.go

- [ ] should not create deployment, service, and configmap
- [ ] should skip downstream dependents when an upstream includeWhen is false
- [ ] should propagate exclusion when includeWhen depends on another resource
- [ ] should wait for status-backed includeWhen before creating dependent resources

### instance_cluster_scoped_test.go

- [ ] should reconcile a cluster-scoped instance with child resources and external refs

### instance_conditions_test.go

- [ ] should have correct hierarchical conditions on successful instance reconciliation
- [ ] should show proper condition progression during deletion

### instance_conflict_test.go

- [ ] should prevent cross-RGD reconciliation using instance GVK labels

### instance_custom_conditions_test.go

- [ ] writes author-defined conditions to status.conditions
- [ ] preserves lastTransitionTime when condition status is unchanged
- [ ] emits kro built-in conditions when no conditions: block is present
- [ ] honors an author-defined Ready that overrides kro's lifecycle Ready
- [ ] evaluates conditions against schema.spec fields and updates them when spec changes
- [ ] runtime.condition reads kro's internal value, not the author's wire override
- [ ] drops runtime-duplicate condition types, sets state=Error, keeps survivors
- [ ] preserves lastTransitionTime for author conditions overriding built-in types
- [ ] preserves a previously written condition while its data is pending
- [ ] removes leftover author conditions after the conditions block is removed

### instance_expression_isolation_test.go

- [ ] carries a literal ${...} in an instance spec field through to a managed resource
- [ ] reconciles an instance whose annotations contain a literal ${...}
- [ ] does not resolve an instance value that names one of the definition's resources

### instance_resource_watch_test.go

- [ ] watch behavior on Secret

### kubernetes_time_test.go

- [ ] should populate status.creationTimestamp from secret.metadata.creationTimestamp

### lifecycle_test.go

- [ ] should handle updates to instance resources correctly

### metadata_fields_test.go

- [ ] should access all metadata fields including namespace in CEL expressions

### nested_test.go

- [ ] should handle nested ResourceGraphDefinition lifecycle
- [ ] should dynamically create RGDs with different schema field types

### omit_test.go

- [ ] should omit a ConfigMap data field when omit() is triggered
- [ ] should omit array elements when omit() is triggered

### readiness_test.go

- [ ] should should wait for the last node in the DAG to be ready

### reconcile_error_metrics_test.go

- [ ] does not count a retryable resource apply failure as a reconcile error

### recover_test.go

- [ ] should recover from invalid state and use latest valid configuration

### resource_ownership_test.go

- [ ] applies managed resources under a stable field manager
- [ ] does not let two instances silently overwrite the same resource

### resource_pruning_test.go

- [ ] prunes a removed resource while another resource is not ready

### resource_shape_compatibility_test.go

- [ ] templates a resource whose apiVersion is outside the vN convention
- [ ] resolves an external reference whose apiVersion is outside the vN convention
- [ ] creates a CustomResourceDefinition declared by a definition
- [ ] names the unresolved field when a cluster-scoped instance's child namespace is empty

### schema_aware_cel_test.go

- [ ] should convert Secret data bytes so string() works at runtime

### status_array_projection_test.go

- [ ] projects a top-level array of expressions as an array
- [ ] projects an array nested under an object as an array

### status_schema_ref_test.go

- [ ] should populate status from schema spec field only
- [ ] should populate status mixing resource field and schema spec field
- [ ] should reflect updated schema spec value in status

### status_test.go

- [ ] should interpolate string templates in instance status
- [ ] should only show status fields when all referenced resources are available

### terminating_managed_resource_test.go

- [ ] should not create downstream resources while a managed resource is terminating

### two_var_comprehensions_test.go

- [ ] should evaluate transformMap, transformMapEntry, and transformList in resource templates

### unknown_fields_test.go

- [ ] should allow instances with arbitrary unknown nested fields and resolve references
- [ ] should not panic when the second RGD embeds the first RGD

### validation_test.go

- [ ] should reject invalid values at admission
- [ ] should reject invalid kind names
- [ ] should not panic when deleting an inactive ResourceGraphDefinition

### webhook_denial_test.go

- [ ] should report webhook denial in status and recover when policy is removed
