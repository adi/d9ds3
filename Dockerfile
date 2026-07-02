# Build a static d9ds3 binary and ship it on a minimal base image.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /d9ds3 ./cmd/d9ds3

FROM gcr.io/distroless/static-debian12
COPY --from=build /d9ds3 /usr/local/bin/d9ds3
# S3 API (gateway/standalone) and storage data-plane / Raft ports.
EXPOSE 8080 8001 9001
ENTRYPOINT ["/usr/local/bin/d9ds3"]
