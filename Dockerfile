# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.25 AS build
WORKDIR /src

# Download modules first so this layer is cached across source changes.
COPY go.mod ./
RUN go mod download

COPY . .
# Static binary: no cgo, so it runs on a distroless image with no libc.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/lsmserver ./cmd/lsmserver

# Data directory owned by the distroless "nonroot" user (uid 65532), so a
# fresh named volume mounted at /data inherits writable ownership.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# ---- runtime ----
# distroless/static: no shell, no package manager, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/lsmserver /usr/local/bin/lsmserver
COPY --from=build --chown=65532:65532 /out/data /data

ENV LSM_ADDR=:8080 \
    LSM_DATA_DIR=/data \
    LSM_SYNC=group
# LSM_AUTH_TOKEN must be supplied at run time; the server refuses to start without it.

EXPOSE 8080
VOLUME ["/data"]
# Non-root by default. Platforms that mount a root-owned volume at /data
# (e.g. Fly.io) can build with --build-arg RUNTIME_USER=root; the process
# still runs inside its own isolated VM/container.
ARG RUNTIME_USER=nonroot:nonroot
USER ${RUNTIME_USER}
ENTRYPOINT ["/usr/local/bin/lsmserver"]
