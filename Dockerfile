FROM golang:1.21 AS builder
LABEL fnkit.fn="true"
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/main.go

FROM gcr.io/distroless/static-debian11
COPY --from=builder /app/server /server

# Function target name — also used as Valkey config key
# e.g. pglog → reads fnkit:config:pglog from cache
ENV FUNCTION_TARGET=pglog

# PostgreSQL connection (external)
ENV DATABASE_URL=postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable

# Shared cache (Valkey/Redis) — available to all functions on fnkit-network
# Also used for config storage (fnkit:config:<function-target>)
ENV CACHE_URL=redis://fnkit-cache:6379
ENV CACHE_KEY_PREFIX=uns

EXPOSE 8080
CMD ["/server"]
