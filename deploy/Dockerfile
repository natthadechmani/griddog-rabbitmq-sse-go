# syntax=docker/dockerfile:1

# Single multi-stage Dockerfile shared by both Go services.
# Select which binary to build with --build-arg SERVICE=gateway|processing.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=gateway

# Pinned (was @latest): orchestrion v1.13.0+ bundles contribs needing dd-trace-go v2.10+,
# which conflicts with the v2.9.1 pinned in go.mod ("orchestrion pin" -> go mod tidy fails).
RUN go install github.com/DataDog/orchestrion@v1.12.1
RUN orchestrion pin
RUN CGO_ENABLED=0 GOOS=linux orchestrion go build -trimpath -o /out/app ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=build /out/app /app/app
ENTRYPOINT ["/app/app"]
