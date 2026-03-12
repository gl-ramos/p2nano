# ── Stage 1: compilar o binário Go ────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copia dependências primeiro (cache de camadas)
COPY go.mod go.sum ./
RUN go mod download

# Copia o código fonte e o frontend (necessário para o go:embed)
COPY main.go .
COPY frontend/ ./frontend/

# Build estático: sem cgo, binário autocontido com frontend embutido
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /p2nano .

# ── Stage 2: imagem final mínima ──────────────────────────────────────────
FROM alpine:3.20

# Certificados raiz (necessários para conexões TLS de saída, ex: STUN)
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Apenas o binário — frontend já está embutido dentro dele
COPY --from=builder /p2nano ./p2nano

# Porta padrão
EXPOSE 8080

# O binário serve HTTP + WebSocket + frontend estático (tudo embutido)
ENTRYPOINT ["./p2nano"]
