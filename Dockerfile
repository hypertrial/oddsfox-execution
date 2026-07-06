FROM golang:1.23-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/oddsfox-live .

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/oddsfox-live /usr/local/bin/oddsfox-live

EXPOSE 8787
ENTRYPOINT ["oddsfox-live"]
CMD ["-addr", "0.0.0.0:8787", "-artifact-dir", "/artifacts"]
