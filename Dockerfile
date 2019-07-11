FROM minio/minsql:deps

WORKDIR /usr/src/minsql
COPY . .

RUN cargo install --path .

FROM alpine:3.9

COPY --from=0 /root/.cargo/bin/minsql /usr/bin/minsql

CMD ["minsql"]
