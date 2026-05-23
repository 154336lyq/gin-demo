FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/web3-grpc-server ./cmd/web3-grpc-server

FROM debian:bookworm-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/web3-grpc-server /app/web3-grpc-server

EXPOSE 50051

ENTRYPOINT ["/app/web3-grpc-server"]

