# syntax=docker/dockerfile:1

# Single multi-stage Dockerfile shared by both Go services.
# Select which binary to build with --build-arg SERVICE=gateway|processing.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=gateway
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/app ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=build /out/app /app/app
ENTRYPOINT ["/app/app"]
