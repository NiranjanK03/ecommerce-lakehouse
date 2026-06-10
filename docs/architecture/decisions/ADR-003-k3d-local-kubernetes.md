# ADR-003: k3d for Local Kubernetes

**Status:** Accepted  
**Date:** 2024-01

## Context

Spark on Kubernetes, Strimzi/Kafka, Airflow with KubernetesExecutor, and Trino all require a real Kubernetes cluster with PVC support, namespace isolation, RBAC, and CRD registration. The development environment is a MacBook Pro with 16 GB RAM.

Options considered: Docker Compose, minikube, kind, k3d.

## Decision

Use k3d (k3s in Docker).

**Why not Docker Compose:**  
Compose is excellent for 3–5 services but does not support Kubernetes primitives (CRDs, RBAC, PodTemplates, ServiceAccounts). The Spark k8s operator requires CRDs. Airflow's KubernetesExecutor submits PodTemplates directly to the API server.

**Why k3d over minikube:**  
minikube runs a VM (even with the Docker driver, it runs a separate containerd). k3d runs k3s nodes as Docker containers sharing the host's Docker daemon — image pulls from `registry.localhost:5001` are instant (layer cache is shared). minikube's tunnel for LoadBalancer services adds complexity we avoid by using port-forward.

**Why k3d over kind:**  
kind is the CNCF-blessed tool for CI/CD cluster testing but is not designed for persistent workloads. k3d includes a persistent volume provisioner out of the box (local-path). kind requires an explicit PV provisioner install.

**Resource allocation:**  
1 server + 3 agents. Idle consumption is ~800 MB RAM. Under full load (Spark + Airflow + Trino + MinIO) it peaks at ~10–12 GB, which fits in 16 GB with macOS overhead.

## Consequences

**Good:** Sub-60-second cluster creation. Port-forward works without extras. PVC support out of the box. Local registry eliminates DockerHub rate limits.

**Bad:** k3s ships with an older containerd and CNI than upstream Kubernetes. Not a concern for this stack — all images support standard containerd.
