FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum for dependency caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire source code
COPY . .

# Build the custom OPA binary with natsstore plugin
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o opa-nats ./examples/handledpaths/main.go

FROM alpine:latest

# Install ca-certificates for TLS connections
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the built binary from builder stage
COPY --from=builder /app/opa-nats .

# Create directories for config and policies
RUN mkdir -p /config /policies

EXPOSE 8181

# Run OPA server with config
CMD ["./opa-nats", "run", "--server", "--config-file=/config/config.yaml", "/policies"]