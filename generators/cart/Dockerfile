FROM golang:1.23-alpine AS builder

WORKDIR /app

# Копируем весь проект
COPY . .

# Переходим в директорию сервиса и собираем
WORKDIR /app/generators/cart
RUN go mod download
RUN go build -o /cart-service

FROM alpine:3.18

RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /cart-service ./

RUN adduser -D -u 1000 appuser
USER appuser

CMD ["./cart-service"]