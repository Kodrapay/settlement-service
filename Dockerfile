# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go mod files first
COPY go.mod .
COPY go.sum .

# Copy source code
COPY . .

# Build the application without downloading dependencies
# (they should be in the go.sum already, or we vendor them)
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o settlement-service ./cmd/settlement-service

# Final stage
FROM alpine:latest

# Install curl for health checks
RUN apk --no-cache add curl

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/settlement-service .

EXPOSE 7000

CMD ["./settlement-service"]
