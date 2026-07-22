# Custom Datadog Agent image: the base agent plus the EMQX integration
# (datadog-emqx, from integrations-extras), which is NOT bundled in the base image.
# Once installed, the `emqx` check is configured at runtime via Autodiscovery labels
# on the emqx service in docker-compose.yml — the same pattern used for rabbitmq.
FROM registry.datadoghq.com/agent:latest
RUN agent integration install -r -t datadog-emqx==1.1.0
