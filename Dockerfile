# Build stage
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/click-sink ./cmd/click-sink

# Runtime stage
FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /out/click-sink /app/click-sink
# Expose default ports for API and metrics; container orchestrator publishes as needed
EXPOSE 8081 8082 2112
ENTRYPOINT ["/app/click-sink"]
