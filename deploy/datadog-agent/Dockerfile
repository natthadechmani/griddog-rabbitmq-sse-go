# Custom Datadog Agent image. The ONLY reason it's custom is the datadog-emqx
# integrations-extras check (EMQX Prometheus metrics via Autodiscovery), which the base
# image doesn't bundle — so we install it here.
#
# Standard (non-`-full`) image: `-full` only adds DDOT, the embedded OTel Collector, which
# we do NOT use — DDOT is supported only on Kubernetes/ECS Fargate and crash-loops on
# docker-compose. EMQX's native OTel traces are ingested by the agent's BUILT-IN OTLP
# receiver (DD_OTLP_CONFIG_* in docker-compose.yml), which lives in the CORE agent.
FROM registry.datadoghq.com/agent:latest
RUN agent integration install -r -t datadog-emqx==1.1.0
