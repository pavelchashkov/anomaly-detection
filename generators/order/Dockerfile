FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY . .

WORKDIR /app/generators/order
RUN go mod download
RUN go build -o /order-service

FROM alpine:3.18

RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /order-service ./

RUN adduser -D -u 1000 appuser
USER appuser

CMD ["./order-service"]