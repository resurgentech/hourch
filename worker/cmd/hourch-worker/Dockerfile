# hourch-worker, small container variant — build from the repository root:
#   docker build -f worker/cmd/hourch-worker/Dockerfile .
# (`just image` in worker does this, passes VERSION and stages IMAGE_CA_CERT.)
# Runtime is archlinux:base with only what Arch package-release jobs need
# (pacman tools incl. repo-add/vercmp/bsdtar, gnupg, openssh). There is no
# docker CLI: this image cannot serve container jobs.
FROM golang:1.25-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN cd worker && CGO_ENABLED=0 go build -ldflags "-X github.com/resurgentech/hourch/worker.Version=${VERSION}" -o /out/hourch-worker ./cmd/hourch-worker

FROM archlinux:base
RUN pacman -Syu --noconfirm --needed gnupg openssh ca-certificates \
 && pacman -Scc --noconfirm
COPY worker/cmd/hourch-worker/extra-ca/ /etc/ca-certificates/trust-source/anchors/
RUN trust extract-compat
COPY --from=build /out/hourch-worker /usr/local/bin/hourch-worker
COPY worker/worker.yaml.example /etc/hourch/worker.yaml.example
ENTRYPOINT ["/usr/local/bin/hourch-worker"]
CMD ["--config", "/etc/hourch/worker.yaml"]
