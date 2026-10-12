# The kind development harness builds these Linux binaries from the checkout.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
COPY console /usr/local/bin/onlyboxes-console
COPY worker-kubernetes /usr/local/bin/onlyboxes-worker-kubernetes
ENV HOME=/tmp UV_CACHE_DIR=/tmp/uv-cache
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/onlyboxes-worker-kubernetes"]
