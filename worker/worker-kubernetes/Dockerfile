FROM golang:1.27 AS build
WORKDIR /src
COPY api ./api
COPY worker/worker-kubernetes ./worker/worker-kubernetes
WORKDIR /src/worker/worker-kubernetes
RUN CGO_ENABLED=0 go build -trimpath -o /worker ./cmd/worker-kubernetes
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /worker /worker
USER 1000:1000
ENTRYPOINT ["/worker"]
