# One Dockerfile for both services: docker compose passes CMD=api or CMD=worker.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/app /app
USER app
ENTRYPOINT ["/app"]
