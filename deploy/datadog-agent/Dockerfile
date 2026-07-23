# Custom Datadog Agent image, built on the `-full` tag because it bundles the DDOT
# (Datadog Distribution of the OpenTelemetry Collector) used to ingest EMQX's native
# OTel traces (enabled via DD_OTELCOLLECTOR_ENABLED + otel-config.yaml in compose).
# The base image does NOT bundle the datadog-emqx integrations-extras check (used for
# EMQX Prometheus metrics via Autodiscovery), so we still install it here.
FROM registry.datadoghq.com/agent:latest-full
RUN agent integration install -r -t datadog-emqx==1.1.0
