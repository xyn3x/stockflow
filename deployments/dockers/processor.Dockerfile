# ── Build stage ──────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/processor \
    ./cmd/processor

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache wget

COPY --from=builder /bin/processor /processor
COPY configs/processor.yaml /configs/processor.yaml

ENV PROCESSOR_CONFIG=/configs/processor.yaml
ENTRYPOINT ["/processor"]
