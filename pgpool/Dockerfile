FROM ubuntu:jammy AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG PGPOOL_VERSION=4.6.7
ARG PGPOOL_TAG=V4_6_7

RUN apt-get update -y && apt-get install -y \
    build-essential \
    ca-certificates \
    wget \
    curl \
    flex \
    bison \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    libpam0g-dev \
    libxml2-dev \
    libpq-dev \
    libevent-dev \
    libmemcached-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN set -eux; \
    curl -fL -o pgpool.tar.gz "https://github.com/pgpool/pgpool2/archive/refs/tags/${PGPOOL_TAG}.tar.gz"; \
    tar tzf pgpool.tar.gz >/dev/null; \
    src_dir="$(tar tzf pgpool.tar.gz | head -1 | cut -d/ -f1)"; \
    tar xzf pgpool.tar.gz; \
    cd "$src_dir"; \
    if [ ! -x ./configure ]; then autoreconf -fi; fi; \
    ./configure --prefix=/usr/local --with-openssl; \
    make -j"$(nproc)"; \
    make install

FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y \
    ca-certificates \
    tini \
    procps \
    libssl3 \
    libreadline8 \
    zlib1g \
    libxml2 \
    libpq5 \
    libevent-2.1-7 \
    libmemcached11 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -m -d /var/lib/pgpool -s /usr/sbin/nologin pgpool && \
    mkdir -p /etc/pgpool2 /var/run/pgpool /var/log/pgpool && \
    chown -R pgpool:pgpool /etc/pgpool2 /var/run/pgpool /var/log/pgpool /var/lib/pgpool

COPY --from=builder /usr/local /usr/local
COPY ./pgpool/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 5432 9898
USER pgpool
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]

