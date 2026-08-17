# Custom Datadog Agent image. The ONLY reason it's custom is the datadog-emqx
# integrations-extras check (EMQX Prometheus metrics via Autodiscovery), which the base
# image doesn't bundle — so we install it here.
#
# EMQX's native OTel traces are ingested by the agent's BUILT-IN OTLP receiver
# (DD_OTLP_CONFIG_* in docker-compose.yml), which lives in the core agent — NOT the DDOT
# embedded collector. So the `-full` tag is not strictly required anymore (DDOT is only
# supported on Kubernetes/ECS Fargate and crash-loops on docker-compose); it's retained
# only to avoid a rebuild and is a harmless superset.
FROM registry.datadoghq.com/agent:latest-full
RUN agent integration install -r -t datadog-emqx==1.1.0
