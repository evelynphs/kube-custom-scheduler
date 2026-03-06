FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN --mount=type=cache,target=/go/pkg \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /bin/kube-custom-scheduler

FROM alpine:3.18


RUN apk add --no-cache python3 py3-numpy

COPY --from=builder /bin/kube-custom-scheduler /bin/kube-custom-scheduler
COPY pi_estimation.py /app/pi_estimation.py
WORKDIR /app

CMD ["/bin/kube-custom-scheduler"]