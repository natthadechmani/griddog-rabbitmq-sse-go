# Griddog Kubernetes Operational Guide

This guide collects the day-to-day commands for checking, stopping, deleting, and restarting the local
`minikube` environment for this repo.

Run commands from the repo root:

```bash
cd /Users/natthadech.manichot/Desktop/project/sandbox/griddog-rabbitmq-sse-go
```

## Check What Is Running

Check `minikube` and the active Kubernetes context:

```bash
minikube status
kubectl config current-context
```

List everything in the cluster:

```bash
kubectl get namespaces
kubectl get all -A
```

Check only the Griddog app:

```bash
kubectl -n griddog get pods,svc,deploy
kubectl -n griddog get events --sort-by=.lastTimestamp
```

Check Datadog:

```bash
kubectl -n datadog get datadogagent,pods,svc,deploy
```

Inspect recent app logs:

```bash
kubectl -n griddog logs deploy/gateway-backend --tail=100
kubectl -n griddog logs deploy/processing-backend --tail=100
kubectl -n griddog logs deploy/rabbitmq --tail=100
kubectl -n griddog logs deploy/mysql --tail=100
```

Inspect Datadog Agent status:

```bash
AGENT_POD="$(kubectl -n datadog get pod -l app=datadog-agent -o jsonpath='{.items[0].metadata.name}')"
kubectl exec -n datadog "$AGENT_POD" -c agent -- agent status
```

Check for local port-forwards:

```bash
ps aux | grep '[k]ubectl.*port-forward'
```

Stop all local port-forwards:

```bash
pkill -f 'kubectl.*port-forward'
```

## Pause The App

This stops Griddog workloads but keeps the cluster and Datadog running.

```bash
kubectl -n griddog scale deploy --all --replicas=0
kubectl -n griddog get pods
```

Start the app deployments again:

```bash
kubectl -n griddog scale deploy/mysql deploy/rabbitmq --replicas=1
kubectl -n griddog rollout status deploy/mysql
kubectl -n griddog rollout status deploy/rabbitmq

kubectl -n griddog scale deploy/processing-backend deploy/gateway-backend deploy/frontend --replicas=1
kubectl -n griddog rollout status deploy/processing-backend
kubectl -n griddog rollout status deploy/gateway-backend
kubectl -n griddog rollout status deploy/frontend
```

Note: the current MySQL manifest does not use a PersistentVolumeClaim. If the MySQL pod is recreated,
the demo database contents may be lost.

## Delete Only The Griddog App

This removes RabbitMQ, MySQL, gateway, processing, frontend, and the `griddog` namespace.
Datadog remains installed.

```bash
kubectl delete namespace griddog
```

Deploy the app again:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/mysql.yaml -f k8s/rabbitmq.yaml
kubectl apply -f k8s/processing-backend.yaml -f k8s/gateway-backend.yaml -f k8s/frontend.yaml

kubectl -n griddog get pods -w
```

## Delete Griddog And Datadog

This removes the app and Datadog resources from the cluster.

```bash
kubectl delete namespace griddog

kubectl delete -f k8s/datadog-agent.yaml --ignore-not-found
helm uninstall datadog-operator -n datadog
kubectl delete namespace datadog
```

## Stop Or Delete Minikube

Stop the cluster but keep its state:

```bash
minikube stop
```

Delete the cluster completely:

```bash
minikube delete
```

Use `minikube delete` when you want a clean local Kubernetes environment. This removes cluster state,
including all namespaces, pods, services, Datadog resources, and in-cluster MySQL data.

## Start From Scratch

Start `minikube`:

```bash
minikube start --driver=docker --container-runtime=containerd --cpus=4 --memory=8192
```

Build local images:

```bash
docker build -t griddog/gateway:dev -f deploy/Dockerfile --build-arg SERVICE=gateway .
docker build -t griddog/processing:dev -f deploy/Dockerfile --build-arg SERVICE=processing .
docker build -t griddog/frontend:dev -f deploy/frontend/Dockerfile .
```

Load images into `minikube`:

```bash
minikube image load griddog/gateway:dev
minikube image load griddog/processing:dev
minikube image load griddog/frontend:dev
```

Install or reconcile Datadog:

```bash
helm repo add datadog https://helm.datadoghq.com
helm repo update

kubectl create namespace datadog --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install datadog-operator datadog/datadog-operator -n datadog

kubectl create secret generic datadog-secret -n datadog \
  --from-literal api-key="$(grep '^DD_API_KEY=' .env | cut -d= -f2)" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f k8s/datadog-agent.yaml
```

Deploy the app:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/mysql.yaml -f k8s/rabbitmq.yaml
kubectl apply -f k8s/processing-backend.yaml -f k8s/gateway-backend.yaml -f k8s/frontend.yaml
```

Wait for healthy rollouts:

```bash
kubectl -n datadog rollout status deploy/datadog-operator
kubectl -n datadog get datadogagent,pods

kubectl -n griddog rollout status deploy/mysql
kubectl -n griddog rollout status deploy/rabbitmq
kubectl -n griddog rollout status deploy/processing-backend
kubectl -n griddog rollout status deploy/gateway-backend
kubectl -n griddog rollout status deploy/frontend
```

Open the app locally with port-forwarding in separate terminals:

```bash
kubectl -n griddog port-forward svc/frontend 18088:80
kubectl -n griddog port-forward svc/gateway-backend 18080:8080
```

Use:

```text
UI:  http://localhost:18088
API: http://localhost:18080/api/health
```

## Generate Test Traffic

RabbitMQ flow:

```bash
curl -s -X POST http://localhost:18080/api/rabbitmq-call \
  -H 'content-type: application/json' \
  -d '{"value":7}'
```

HTTP flow:

```bash
curl -s -X POST http://localhost:18080/api/http-call \
  -H 'content-type: application/json' \
  -d '{"value":7}'
```

Read recent RabbitMQ flow messages:

```bash
curl -s 'http://localhost:18080/api/messages?flow=rabbitmq'
```

Read recent HTTP flow messages:

```bash
curl -s 'http://localhost:18080/api/messages?flow=http'
```

## Useful Reconcile Commands

Reload only app manifests:

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/mysql.yaml -f k8s/rabbitmq.yaml
kubectl apply -f k8s/processing-backend.yaml -f k8s/gateway-backend.yaml -f k8s/frontend.yaml
```

Restart only app pods:

```bash
kubectl rollout restart deploy/gateway-backend deploy/processing-backend deploy/frontend -n griddog
```

Restart infrastructure pods:

```bash
kubectl rollout restart deploy/mysql deploy/rabbitmq -n griddog
```

Reload only Datadog:

```bash
kubectl apply -f k8s/datadog-agent.yaml
kubectl -n datadog rollout status deploy/datadog-operator
kubectl -n datadog get datadogagent,pods
```

