# hourchd — build from the repository root:
#   docker build -f daemon/cmd/hourchd/Dockerfile .
# (`just image` in daemon does this and stages IMAGE_CA_CERT.)
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
RUN cd daemon && CGO_ENABLED=0 go build -o /out/hourchd ./cmd/hourchd

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY daemon/cmd/hourchd/extra-ca/ /usr/local/share/ca-certificates/extra/
RUN update-ca-certificates
COPY --from=build /out/hourchd /usr/local/bin/hourchd
COPY daemon/config.yaml.example /etc/hourchd/config.yaml.example
VOLUME ["/var/lib/hourchd"]
EXPOSE 8093
ENTRYPOINT ["/usr/local/bin/hourchd"]
CMD ["serve", "--config", "/etc/hourchd/config.yaml"]
