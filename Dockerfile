ARG BASE_IMAGE=golang:1.26
FROM ${BASE_IMAGE} AS builder
ARG TARGETOS=linux
ARG TARGETARCH
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o manager ./cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /workspace/manager /manager
COPY LICENSE /LICENSE
USER 65532:65532
ENTRYPOINT ["/manager"]
