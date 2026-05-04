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
    -o /bin/simulator \
    ./cmd/simulator

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache wget

COPY --from=builder /bin/simulator /simulator
COPY configs/simulator.yaml /configs/simulator.yaml

EXPOSE 8081
ENV SIMULATOR_CONFIG=/configs/simulator.yaml
ENTRYPOINT ["/simulator"]
