# syntax=docker/dockerfile:1


# Builder
FROM golang:1.17-alpine as builder

WORKDIR /build

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /rabbit-stream-forwarder


# Image
FROM alpine

WORKDIR /

COPY --from=builder /rabbit-stream-forwarder /usr/local/bin/.
COPY docker-entrypoint.sh /usr/local/bin/.
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]