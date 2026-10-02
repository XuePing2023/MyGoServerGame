# ServerGame 多阶段构建
FROM golang:1.27-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o servergame ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache tzdata ca-certificates
WORKDIR /app
COPY --from=builder /build/servergame ./servergame
COPY config.yaml ./config.yaml

ENV SG_LOG_FILE=/app/logs/server.log
EXPOSE 7001 7002 7003

CMD ["/app/servergame"]
