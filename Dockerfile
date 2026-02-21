FROM golang:1.21 AS builder
LABEL fnkit.fn="true"
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/main.go

FROM gcr.io/distroless/static-debian11
COPY --from=builder /app/server /server

# Function target name — also used as S3 config key
ENV FUNCTION_TARGET=pglog

# S3 config storage
ENV S3_ENDPOINT=
ENV S3_BUCKET=fnkit-config
ENV S3_REGION=us-east-1
ENV S3_ACCESS_KEY=
ENV S3_SECRET_KEY=

# PostgreSQL connection (external)
ENV DATABASE_URL=postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable

# Shared cache (Valkey/Redis) — available to all functions on fnkit-network
ENV CACHE_URL=redis://fnkit-cache:6379
ENV CACHE_KEY_PREFIX=uns

EXPOSE 8080
CMD ["/server"]
