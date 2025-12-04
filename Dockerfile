# Build stage
FROM golang:1.24-alpine AS builder
WORKDIR /app

# Install build dependencies
RUN apk --no-cache add ca-certificates git

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o settlement-service ./cmd/settlement-service

# Runtime stage
FROM alpine:latest
RUN apk --no-cache add ca-certificates curl
WORKDIR /app
COPY --from=builder /app/settlement-service .
EXPOSE 7008
CMD ["./settlement-service"]
