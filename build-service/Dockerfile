# ── Stage 1: compile the build-service binary ────────────────────────────────
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main .

# ── Stage 2: runtime image ────────────────────────────────────────────────────
# We use golang:1.23-alpine (not bare alpine) because the build runner needs:
#   - git     → to clone repos
#   - go      → to run `go build ./...` on the cloned code
#   - ca-certificates → for HTTPS clones (comes with the go image)
FROM golang:1.23-alpine
RUN apk add --no-cache git docker-cli
WORKDIR /app
COPY --from=builder /app/main .
CMD ["./main"]
