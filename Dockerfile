FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./

# PENTING: tidy dulu supaya go.sum lengkap
RUN go mod tidy
RUN go mod download

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-amd64} \
    go build -v -o /out/kube-scheduler ./main.go
