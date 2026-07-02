# Build a static d9ds3 binary and ship it on a minimal base image.
# The builder runs on the native BUILDPLATFORM and cross-compiles to TARGET* so
# multi-arch builds don't pay for QEMU emulation of the Go toolchain.
FROM --platform=$BUILDPLATFORM golang:1.24 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -o /d9ds3 ./cmd/d9ds3

FROM gcr.io/distroless/static-debian12
COPY --from=build /d9ds3 /usr/local/bin/d9ds3
# S3 API (gateway/standalone), storage data-plane, and Raft transport ports.
EXPOSE 8080 8001 9001
ENTRYPOINT ["/usr/local/bin/d9ds3"]
