FROM golang:1.27.1-bookworm@sha256:8d48e12ec56735e9358640898b9d9b9fcca110612ed8a5567438c0a1baa24e66 AS build
WORKDIR /src
COPY api ./api
COPY console ./console
COPY worker/worker-kubernetes ./worker/worker-kubernetes
WORKDIR /src/console
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -o /out/console ./cmd/console
WORKDIR /src/worker/worker-kubernetes
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker-kubernetes
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG SOURCE_REVISION
LABEL org.opencontainers.image.source="https://github.com/reonokiy/onlyboxes" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.description="OnlyBoxes Console and native Kubernetes Worker"
COPY --from=build /out/console /console
COPY --from=build /out/worker /worker
USER 1000:1000
ENTRYPOINT ["/worker"]
