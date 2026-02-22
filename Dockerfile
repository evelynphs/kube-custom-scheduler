FROM golang:1.25-alpine3.18 as builder

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

COPY --from=builder /bin/kube-custom-scheduler /bin/kube-custom-scheduler

CMD ["/bin/kube-custom-scheduler"]