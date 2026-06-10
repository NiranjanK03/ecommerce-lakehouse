# ADR-004: Bitnami Kafka Chart (not Strimzi)

**Status:** Accepted  
**Date:** 2024-01

## Context

Kafka needs to run on Kubernetes. Two production-grade options exist: the Bitnami `kafka` Helm chart and the Strimzi Kafka operator (CNCF Sandbox project).

## Decision

Use Bitnami `kafka` chart for local development. Strimzi is documented as the production upgrade path.

**Why Bitnami for this project:**  
Strimzi models a Kafka cluster as a `Kafka` custom resource with ~300 lines of spec. It installs a controller manager, the cluster-operator, and registers 12+ CRDs. First-time readiness takes 3–5 minutes. For a portfolio project the operator complexity obscures the data engineering story — readers of the ADRs and runbooks should spend mental energy on the pipeline, not the Kafka controller reconciliation loop.

Bitnami's chart deploys a single StatefulSet. It is ready in 60 seconds and requires 25 lines of values. It supports KRaft (ZooKeeper-less Kafka, available since Kafka 3.3 and Bitnami chart >= 26.x), which removes one more StatefulSet from the cluster.

**When Strimzi would be the right choice:**  
- Multi-tenant Kafka (per-team topic-level RBAC via KafkaUser CRDs)  
- Automated certificate rotation  
- Mirror Maker 2 for cross-cluster replication  
- Production HA with rolling upgrades managed by the operator

None of those requirements apply to a single-developer portfolio project.

## Consequences

**Good:** 60-second Kafka readiness. 25-line values file. KRaft (no ZooKeeper).

**Bad:** No CRD-based topic management (topics are provisioned via the chart's `provisioning` block instead). Manual rolling upgrades if Kafka version needs bumping.
