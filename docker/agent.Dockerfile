ARG MX_SMI_IMAGE=release-ci.daocloud.io/baize/mx-smi:v0.1

FROM --platform=$BUILDPLATFORM m.daocloud.io/docker.io/golang:1.23.2 AS builder

WORKDIR /app

COPY go.mod /app/go.mod
COPY go.sum /app/go.sum

ARG GOPROXY=https://goproxy.cn,direct

RUN go env
RUN go env -w GOPROXY=$GOPROXY
RUN go env -w CGO_ENABLED=0
RUN go mod download

ADD . .

ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -ldflags "-s -w" -o kcover-agent ./cmd/agent

FROM ${MX_SMI_IMAGE} AS metax-tools

# runner
FROM m.daocloud.io/docker.io/ubuntu:24.04

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
	&& apt-get install -y --no-install-recommends chrony \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/kcover-agent kcover-agent
COPY --from=metax-tools /usr/local/bin/mx-smi /usr/local/bin/mx-smi
COPY docker/agent-entrypoint.sh /usr/local/bin/agent-entrypoint.sh

RUN chmod +x /usr/local/bin/mx-smi /usr/local/bin/agent-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/agent-entrypoint.sh"]