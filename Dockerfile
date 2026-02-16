# syntax=docker/dockerfile:1

############################
# 1) Build stage
############################
FROM golang:1.25-bookworm AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod tidy
RUN go mod download
# i need to do this
COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/kube-scheduler ./main.go


############################
# 2) Runtime stage
############################
FROM registry.k8s.io/kube-scheduler:v1.29.0

COPY --from=builder /out/kube-scheduler /usr/local/bin/kube-scheduler

ENTRYPOINT ["/usr/local/bin/kube-scheduler"]
