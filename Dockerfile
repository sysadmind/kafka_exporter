FROM golang:1.14 as builder

COPY *.go go.* /code/
WORKDIR /code

RUN CGO_ENABLED=0 go build -o /kafka_exporter

FROM        quay.io/prometheus/busybox:latest
MAINTAINER  Daniel Qian <qsj.daniel@gmail.com>

COPY --from=builder /kafka_exporter /bin/kafka_exporter

EXPOSE     9308
ENTRYPOINT [ "/bin/kafka_exporter" ]
