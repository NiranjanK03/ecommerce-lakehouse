# ecommerce-lakehouse Makefile
#
# All targets are idempotent — safe to re-run if something fails mid-way.
# Prerequisites: k3d >= 5.6, kubectl, helm >= 3.14, Docker Desktop >= 8 GB RAM

CLUSTER_NAME   := lakehouse
CLUSTER_CONFIG := infrastructure/k8s/cluster/k3d-config.yaml

# Helm chart versions — pinned for reproducibility
MINIO_CHART_VERSION        := 5.4.0
POSTGRES_CHART_VERSION     := 18.6.6
OTEL_CHART_VERSION         := 0.97.0
PROMETHEUS_CHART_VERSION   := 25.21.0
SPARK_OPERATOR_VERSION     := 2.5.0
AIRFLOW_CHART_VERSION      := 1.14.0
STARROCKS_CHART_VERSION    := 1.11.4
TRINO_CHART_VERSION        := 0.29.0

# Spark image — push to Mac host port, pods pull from internal container port
BRONZE_IMAGE_PUSH := localhost:5001/spark-bronze:latest
BRONZE_IMAGE      := registry.localhost:5000/spark-bronze:latest

# Namespaces
NS_INFRA      := infrastructure
NS_STREAMING  := streaming
NS_PROCESSING := processing
NS_SERVING    := serving
NS_OBS        := observability

.DEFAULT_GOAL := help

# ─── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "  ecommerce-lakehouse"
	@echo ""
	@echo "  PHASE 1 — Cluster + core storage"
	@echo "    make cluster-up              Create k3d cluster + local registry"
	@echo "    make deploy-infra            MinIO → Iceberg catalog → PostgreSQL → Kafka"
	@echo ""
	@echo "  PHASE 2 — CDC + streaming pipeline"
	@echo "    make build-bronze-image      Build Spark Docker image"
	@echo "    make deploy-streaming        Spark operator + Debezium + OTel Collector"
	@echo "    make init-bronze-tables      Create Bronze Iceberg tables"
	@echo "    make register-connector      Activate Debezium CDC"
	@echo "    make start-bronze            Start Bronze streaming job"
	@echo ""
	@echo "  PHASE 3 — Orchestration + Silver"
	@echo "    make deploy-orchestration    Airflow"
	@echo "    make init-silver-tables      Create Silver Iceberg tables (via SparkApplication)"
	@echo "    make trigger-silver-dag      Start the Silver → Gold pipeline"
	@echo ""
	@echo "  PHASE 4 — Gold layer (StarRocks + dbt + Trino benchmarks)"
	@echo "    make deploy-week4            StarRocks operator+cluster + Trino"
	@echo "    make init-starrocks          Storage volume + Iceberg catalog (requires port-forward)"
	@echo "    make init-gold-mvs           Create Gold MVs via dbt run (requires port-forward)"
	@echo "    make run-benchmarks          Engine comparison: pipeline + Trino vs StarRocks"
	@echo "    make benchmark-report        Write benchmarks/results/YYYY-MM-DD-benchmark-report.md"
	@echo "    make remove-trino            Remove Trino after benchmarks complete"
	@echo ""
	@echo "  DAILY"
	@echo "    make port-forward            Open all service tunnels"
	@echo "    make pf-stop                 Close port-forwards"
	@echo "    make status                  Show all pod states"
	@echo "    make connector-status        Check Debezium connector health"
	@echo "    make dag-status              Show recent Airflow DAG runs"
	@echo "    make logs-kafka / logs-minio / logs-catalog"
	@echo ""
	@echo "  RESET"
	@echo "    make clean                   Delete cluster + all data"
	@echo ""

# ─── Cluster ──────────────────────────────────────────────────────────────────

.PHONY: cluster-up
cluster-up:
	@echo "==> Creating k3d cluster '$(CLUSTER_NAME)'..."
	@mkdir -p /tmp/k3dvol
	k3d cluster create --config $(CLUSTER_CONFIG)
	@echo "==> Cluster ready. Context: k3d-$(CLUSTER_NAME)"
	kubectl cluster-info

.PHONY: cluster-down
cluster-down:
	k3d cluster delete $(CLUSTER_NAME)

.PHONY: cluster-info
cluster-info:
	kubectl get nodes -o wide
	@echo ""
	kubectl config current-context

# ─── Namespaces ───────────────────────────────────────────────────────────────

.PHONY: namespaces
namespaces:
	kubectl apply -f infrastructure/k8s/namespaces.yaml

# ─── Helm repos ───────────────────────────────────────────────────────────────

.PHONY: helm-repos
helm-repos:
	helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
	helm repo add minio   https://charts.min.io/             2>/dev/null || true
	helm repo update

.PHONY: helm-repos-obs
helm-repos-obs:
	helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts 2>/dev/null || true
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
	helm repo update

.PHONY: helm-repos-serving
helm-repos-serving:
	helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator 2>/dev/null || true
	helm repo add trinodb   https://trinodb.github.io/charts                          2>/dev/null || true
	helm repo update

# ─── Phase 1: Core storage + catalog ──────────────────────────────────────────

.PHONY: deploy-minio
deploy-minio: namespaces helm-repos
	@echo "==> Deploying MinIO..."
	helm upgrade --install minio minio/minio \
		--namespace $(NS_INFRA) \
		--version $(MINIO_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/minio.yaml \
		--wait --timeout 5m
	@echo "==> MinIO ready"

.PHONY: deploy-catalog
deploy-catalog: namespaces
	@echo "==> Deploying Iceberg REST Catalog..."
	kubectl apply -f infrastructure/k8s/helm/charts/iceberg-rest/deployment.yaml
	kubectl rollout status deployment/iceberg-rest-catalog \
		-n $(NS_INFRA) --timeout=3m
	@echo "==> Iceberg REST Catalog ready"

.PHONY: deploy-postgres
deploy-postgres: namespaces helm-repos
	@echo "==> Creating Postgres init-scripts ConfigMap..."
	kubectl create configmap postgres-init-scripts \
		--from-file=01-schema.sql=infrastructure/k8s/helm/values/postgres/init-scripts/01-schema.sql \
		--from-file=02-sample-data.sql=infrastructure/k8s/helm/values/postgres/init-scripts/02-sample-data.sql \
		--namespace $(NS_INFRA) \
		--dry-run=client -o yaml | kubectl apply -f -
	@echo "==> Deploying PostgreSQL..."
	helm upgrade --install postgres bitnami/postgresql \
		--namespace $(NS_INFRA) \
		--version $(POSTGRES_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/postgres.yaml \
		--wait --timeout 5m
	@echo "==> PostgreSQL ready"

.PHONY: deploy-kafka
deploy-kafka: namespaces
	@echo "==> Deploying Kafka (KRaft)..."
	kubectl apply -f infrastructure/k8s/helm/charts/kafka/kafka.yaml
	kubectl rollout status statefulset/kafka -n $(NS_STREAMING) --timeout=3m
	@echo "==> Kafka ready"

.PHONY: deploy-infra
deploy-infra: deploy-minio deploy-postgres deploy-catalog deploy-kafka
	@echo ""
	@echo "==> Core infrastructure deployed."
	@echo "    Run 'make port-forward' to access services."

# ─── Phase 2: CDC + streaming pipeline ────────────────────────────────────────

.PHONY: deploy-spark-operator
deploy-spark-operator: namespaces helm-repos-obs
	@echo "==> Deploying Spark on Kubernetes operator..."
	helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
	helm repo update
	helm upgrade --install spark-operator spark-operator/spark-operator \
		--namespace $(NS_PROCESSING) \
		--version $(SPARK_OPERATOR_VERSION) \
		--values infrastructure/k8s/helm/values/spark-operator.yaml \
		--wait --timeout 3m
	@echo "==> Spark operator ready"

.PHONY: deploy-debezium
deploy-debezium: namespaces
	@echo "==> Deploying Debezium Kafka Connect..."
	kubectl apply -f infrastructure/k8s/helm/charts/debezium/deployment.yaml
	kubectl rollout status deployment/debezium-connect \
		-n $(NS_STREAMING) --timeout=3m
	@echo "==> Debezium Connect ready on port 8083"

.PHONY: deploy-otel-collector
deploy-otel-collector: namespaces helm-repos-obs
	@echo "==> Deploying OTel Collector + Prometheus..."
	helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
		--namespace $(NS_OBS) \
		--version $(OTEL_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/otel-collector.yaml \
		--wait --timeout 3m
	helm upgrade --install prometheus prometheus-community/prometheus \
		--namespace $(NS_OBS) \
		--version $(PROMETHEUS_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/prometheus.yaml \
		--wait --timeout 5m
	@echo "==> OTel Collector + Prometheus ready"

.PHONY: deploy-postgres-exporter
deploy-postgres-exporter: namespaces
	@echo "==> Deploying postgres_exporter..."
	kubectl apply -f infrastructure/k8s/helm/charts/postgres-exporter/deployment.yaml
	kubectl rollout status deployment/postgres-exporter -n $(NS_INFRA) --timeout=2m
	@echo "==> postgres_exporter ready"

.PHONY: build-bronze-image
build-bronze-image:
	@echo "==> Building Bronze Spark image..."
	docker build \
		-t $(BRONZE_IMAGE_PUSH) \
		-f pipelines/spark/docker/Dockerfile \
		pipelines/spark/
	@echo "==> Pushing to local registry..."
	docker push $(BRONZE_IMAGE_PUSH)
	@echo "==> Bronze image ready (pods pull from $(BRONZE_IMAGE))"

.PHONY: init-bronze-tables
init-bronze-tables:
	@echo "==> Initialising Bronze Iceberg tables via REST API..."
	kubectl delete pod bronze-init-rest -n $(NS_PROCESSING) 2>/dev/null || true
	kubectl create configmap bronze-init-rest-script -n $(NS_PROCESSING) \
		--from-file=iceberg-init.sh=infrastructure/k8s/jobs/iceberg-init.sh \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f infrastructure/k8s/jobs/bronze-init-rest.yaml
	kubectl wait pod/bronze-init-rest -n $(NS_PROCESSING) \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s
	@echo "==> Bronze tables created"
	@kubectl logs bronze-init-rest -n $(NS_PROCESSING) | tail -10

.PHONY: start-bronze
start-bronze:
	@echo "==> Applying Bronze SparkApplication CRD..."
	kubectl apply -f pipelines/spark/applications/bronze-streaming.yaml
	@echo "==> Watching Bronze streaming job start..."
	kubectl get sparkapplication bronze-cdc-ingestion -n $(NS_PROCESSING) -w &
	sleep 10
	kubectl get pods -n $(NS_PROCESSING) -l spark-role=driver

.PHONY: register-connector
register-connector:
	@echo "==> Registering Debezium connector (requires port-forward on 8083)..."
	bash pipelines/debezium/scripts/register-connector.sh

.PHONY: connector-status
connector-status:
	@curl -s http://localhost:8083/connectors/olist-postgres-connector/status | python3 -m json.tool

.PHONY: deploy-streaming
deploy-streaming: deploy-otel-collector deploy-postgres-exporter deploy-spark-operator deploy-debezium
	@echo ""
	@echo "==> CDC + streaming pipeline deployed."
	@echo ""
	@echo "  Next steps:"
	@echo "    1. make build-bronze-image"
	@echo "    2. make port-forward"
	@echo "    3. make init-bronze-tables"
	@echo "    4. make register-connector"
	@echo "    5. make start-bronze"

# ─── Phase 3: Orchestration + Silver ──────────────────────────────────────────

.PHONY: deploy-airflow
deploy-airflow: namespaces helm-repos-obs
	@echo "==> Creating Airflow metadata database..."
	kubectl exec -n $(NS_INFRA) $$(kubectl get pods -n $(NS_INFRA) \
		-l app.kubernetes.io/name=postgresql -o name | head -1) -- \
		psql -U olist_user -d postgres \
		-c "CREATE DATABASE airflow;" 2>/dev/null || echo "  (airflow db already exists)"
	@echo "==> Deploying Airflow..."
	helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace orchestration \
		--version $(AIRFLOW_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/airflow.yaml \
		--wait --timeout 10m
	@echo "==> Airflow ready"

.PHONY: sync-dags
sync-dags:
	@echo "==> Syncing DAGs to Airflow PVC..."
	@SCHEDULER_POD=$$(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) && \
	for f in pipelines/airflow/dags/*.py; do \
		kubectl cp "$$f" "orchestration/$${SCHEDULER_POD#pod/}:/opt/airflow/dags/$$(basename $$f)"; \
		echo "  copied $$(basename $$f)"; \
	done

.PHONY: sync-plugins
sync-plugins:
	@echo "==> Syncing plugins to Airflow..."
	@SCHEDULER_POD=$$(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) && \
	for f in pipelines/airflow/plugins/*.py; do \
		kubectl cp "$$f" "orchestration/$${SCHEDULER_POD#pod/}:/opt/airflow/plugins/$$(basename $$f)"; \
		echo "  copied $$(basename $$f)"; \
	done

.PHONY: init-silver-tables
init-silver-tables:
	@echo "==> Initialising Silver Iceberg tables via SparkApplication..."
	kubectl apply -f pipelines/spark/applications/silver-init-tables.yaml
	kubectl wait sparkapplication silver-init-tables -n $(NS_PROCESSING) \
		--for=jsonpath='{.status.applicationState.state}'=COMPLETED --timeout=120s
	@echo "==> Silver tables created"

.PHONY: trigger-silver-dag
trigger-silver-dag:
	@echo "==> Triggering silver_gold_pipeline DAG..."
	kubectl exec -n orchestration \
		$$(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
		airflow dags trigger silver_gold_pipeline
	@echo "==> DAG triggered — open http://localhost:8080 to monitor"

.PHONY: dag-status
dag-status:
	kubectl exec -n orchestration \
		$$(kubectl get pods -n orchestration -l component=scheduler -o name | head -1) -- \
		airflow dags list-runs -d silver_gold_pipeline --limit 5

.PHONY: deploy-orchestration
deploy-orchestration: deploy-airflow
	@$(MAKE) sync-dags
	@$(MAKE) sync-plugins
	@echo ""
	@echo "==> Orchestration layer deployed."
	@echo ""
	@echo "  Next steps:"
	@echo "    1. make port-forward"
	@echo "    2. make init-silver-tables"
	@echo "    3. make trigger-silver-dag"

# ─── Phase 4: Gold layer (StarRocks + dbt + Trino benchmarks) ─────────────────

.PHONY: deploy-starrocks
deploy-starrocks: namespaces helm-repos-serving
	@echo "==> Deploying StarRocks operator + cluster (shared-data mode)..."
	helm upgrade --install kube-starrocks starrocks/kube-starrocks \
		--namespace $(NS_SERVING) \
		--create-namespace \
		--version $(STARROCKS_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/starrocks-operator.yaml \
		--wait --timeout 8m
	@echo "==> StarRocks FE + CN ready"
	kubectl get pods -n $(NS_SERVING) -l "app.kubernetes.io/instance=kube-starrocks"

.PHONY: init-starrocks
init-starrocks:
	@echo "==> Initialising StarRocks storage volume + Iceberg catalog..."
	@echo "    Requires: port-forward on 9030 (run 'make port-forward' first)"
	mysql -h 127.0.0.1 -P 9030 -u root --password='' < scripts/init-starrocks.sql
	@echo "==> StarRocks initialised."

.PHONY: deploy-trino
deploy-trino: namespaces helm-repos-serving
	@echo "==> Deploying Trino (benchmark comparison)..."
	helm upgrade --install trino trinodb/trino \
		--namespace $(NS_SERVING) \
		--version $(TRINO_CHART_VERSION) \
		--values infrastructure/k8s/helm/values/trino.yaml \
		--wait --timeout 5m
	@echo "==> Trino ready at http://localhost:8085 (after port-forward)"

.PHONY: remove-trino
remove-trino:
	@echo "==> Removing Trino (benchmark phase complete)..."
	helm uninstall trino -n $(NS_SERVING) 2>/dev/null || echo "  (trino not installed)"
	@echo "==> Trino removed"

.PHONY: init-gold-mvs
init-gold-mvs:
	@echo "==> Creating Gold materialized views via dbt..."
	@echo "    Requires: port-forward on 9030 (run 'make port-forward' first)"
	cd transform/dbt && dbt run --profiles-dir . --project-dir .
	@echo "==> Gold MVs created."

.PHONY: run-pipeline-benchmark
run-pipeline-benchmark:
	@echo "==> Running pipeline benchmark (Bronze throughput + E2E latency + Silver MERGE)..."
	@echo "    Requires port-forward on 8085 (Trino)"
	python3 -m benchmarks.pipeline_benchmark

.PHONY: run-benchmarks
run-benchmarks: run-pipeline-benchmark
	@echo "==> Running engine comparison benchmarks (Trino vs StarRocks, 3 scenarios)..."
	@echo "    Requires port-forward on 9030 (StarRocks) and 8085 (Trino)"
	python3 -m benchmarks.engine_compare

.PHONY: benchmark-report
benchmark-report:
	@echo "==> Generating full benchmark report..."
	python3 -m benchmarks.report

.PHONY: test-benchmarks
test-benchmarks:
	@echo "==> Running benchmark module unit tests..."
	python3 -m pytest tests/benchmarks/ -v

.PHONY: deploy-week4
deploy-week4: deploy-starrocks deploy-trino
	@echo ""
	@echo "==> Gold layer deployed."
	@echo ""
	@echo "  Next steps (all require 'make port-forward' first):"
	@echo "    make init-starrocks         — storage volume + Iceberg catalog + gold DB"
	@echo "    make init-gold-mvs          — create Gold MVs via dbt run"
	@echo "    make run-benchmarks         — full benchmark (pipeline + Trino vs StarRocks)"
	@echo "    make benchmark-report       — generate showcase Markdown report"
	@echo ""

# ─── Port forwarding ──────────────────────────────────────────────────────────

.PHONY: port-forward
port-forward:
	@bash scripts/port-forward.sh start

.PHONY: pf-stop
pf-stop:
	@bash scripts/port-forward.sh stop

# ─── Status + logs ────────────────────────────────────────────────────────────

.PHONY: status
status:
	@echo "=== Nodes ==="
	@kubectl get nodes -o wide
	@echo ""
	@echo "=== Pods: infrastructure ==="
	@kubectl get pods -n infrastructure -o wide
	@echo ""
	@echo "=== Pods: streaming ==="
	@kubectl get pods -n streaming -o wide
	@echo ""
	@echo "=== Pods: processing ==="
	@kubectl get pods -n processing -o wide
	@echo ""
	@echo "=== Pods: serving ==="
	@kubectl get pods -n serving -o wide
	@echo ""
	@echo "=== PVCs ==="
	@kubectl get pvc -A

.PHONY: logs-kafka
logs-kafka:
	kubectl logs -n $(NS_STREAMING) -l app.kubernetes.io/name=kafka --follow

.PHONY: logs-minio
logs-minio:
	kubectl logs -n $(NS_INFRA) -l app.kubernetes.io/name=minio --follow

.PHONY: logs-catalog
logs-catalog:
	kubectl logs -n $(NS_INFRA) -l app=iceberg-rest-catalog --follow

# ─── Data loading ─────────────────────────────────────────────────────────────

.PHONY: load-olist-data
load-olist-data:
	bash scripts/load-olist-data.sh

# ─── Teardown ─────────────────────────────────────────────────────────────────

.PHONY: clean
clean:
	@bash scripts/port-forward.sh stop 2>/dev/null || true
	k3d cluster delete $(CLUSTER_NAME) 2>/dev/null || true
	rm -rf /tmp/k3dvol
	@echo "==> Clean complete."
