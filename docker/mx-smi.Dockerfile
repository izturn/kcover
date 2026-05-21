FROM cr.metax-tech.com/public-library/maca-pytorch:3.3.0.4-torch2.6-py310-ubuntu24.04-amd64 AS metax-tools

FROM m.daocloud.io/docker.io/ubuntu:24.04

COPY --from=metax-tools /opt/mxdriver/bin/mx-smi /usr/local/bin/mx-smi

RUN chmod +x /usr/local/bin/mx-smi
