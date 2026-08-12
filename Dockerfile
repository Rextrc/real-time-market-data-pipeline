# Build stage.
FROM golang:1.24-alpine AS build

WORKDIR /src

# Dependencies first so a source-only change doesn't re-download the module
# cache on every rebuild.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 gives a fully static binary. The SQLite driver is pure Go
# (modernc.org/sqlite), so there is no C dependency to link against and the
# runtime image can be almost empty.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mdp ./cmd/mdp
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fakevenue ./cmd/fakevenue

# Runtime stage.
FROM alpine:3.20

# Certificates for the TLS handshake with the exchange, and tzdata because
# every timestamp in this system is UTC and should stay that way even if the
# host disagrees.
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 mdp

COPY --from=build /out/mdp /usr/local/bin/mdp
COPY --from=build /out/fakevenue /usr/local/bin/fakevenue

# The archive is the one irreplaceable asset here, so it lives on a volume.
# A container restart must never be able to take the captured history with it.
RUN mkdir -p /data && chown mdp:mdp /data
VOLUME ["/data"]

USER mdp
WORKDIR /data
EXPOSE 8080

ENV TZ=UTC

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- "http://127.0.0.1:${PORT:-8080}/healthz" || exit 1

# Configuration comes from environment variables (MDP_*), not baked-in flags,
# because that is how Railway and most PaaS configure a service. PORT is
# injected by the platform and takes precedence over MDP_HTTP.
ENV MDP_DB=/data/mdp.db \
    MDP_PAPER_STATE=/data/paper.json

ENTRYPOINT ["/usr/local/bin/mdp"]
