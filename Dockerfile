FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Copy go.mod and go.sum for dependency caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire source code
COPY . .

# Build the custom OPA binary with natsstore plugin
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o opa ./cmd/opa-nats

FROM alpine:latest

# Install ca-certificates for TLS connections
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh opa

# Copy the built binary from builder stage
COPY --from=builder /app/opa /usr/local/bin/opa

# Switch to non-root user
USER opa

ENTRYPOINT ["opa"]
