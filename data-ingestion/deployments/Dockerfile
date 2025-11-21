FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git make gcc musl-dev


WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

FROM builder AS market-producer-builder
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
        -ldflags '-extldflags "-static"' \
        -o /app/bin/market-producer ./cmd/market-producer


FROM builder AS new-producer-builder
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
        -ldflags '-extldflags "-static"' \
        -o /app/bin/news-producer ./cmd/news-producer

FROM builder AS social-producer-builder
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
    -ldflags '-extldflags "-static"' \
    -o /app/bin/social-producer ./cmd/social-producer

FROM builder AS sec-producer-builder
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
    -ldflags '-extldflags "-static"' \
    -o /app/bin/sec-filing-producer ./cmd/sec-producer

FROM builder AS alt-data-producer-builder
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
    -ldflags '-extldflags "-static"' \
    -o /app/bin/alt-data-producer ./cmd/alt-data-producer

FROM alpine:latest AS market-producer
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=market-producer-builder /app/bin/market-producer .
COPY configs/config.yaml ./config.yaml
EXPOSE 9090
CMD ["./market-producer"]

FROM alpine:latest AS news-producer
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=news-producer-builder /app/bin/news-producer .
COPY configs/config.yaml ./config.yaml
EXPOSE 9091
CMD ["./news-producer"]

FROM alpine:latest AS social-producer
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=social-producer-builder /app/bin/social-producer .
COPY configs/config.yaml ./config.yaml
EXPOSE 9092
CMD ["./social-producer"]


FROM alpine:latest AS sec-producer
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=sec-producer-builder /app/bin/sec-producer .
COPY configs/config.yaml ./config.yaml
EXPOSE 9093
CMD ["./sec-producer"]

FROM alpine:latest AS alt-data-producer
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=alt-data-producer-builder /app/bin/alt-data-producer .
COPY configs/config.yaml ./config.yaml
EXPOSE 9094
CMD ["./alt-data-producer"]


