# # syntax=docker/dockerfile:1
# ARG PG_MAJOR=17

# # ubuntu:noble is equal to ubuntu:24.0.4(LTS)
# ARG UBUNTU_IMAGE=ubuntu:noble

# FROM ${UBUNTU_IMAGE} AS builder

# ARG DEBIAN_FRONTEND=noninteractive
# ARG PGPOOL_VERSION=4.6.7
# ARG PGPOOL_TAG=V4_6_7

# # psql v17を使用するためにPostgreSQL 公式 APT リポジトリ (PGDG) を設定する
# RUN apt-get update && apt-get install -y ca-certificates curl \
# && install -d /usr/share/postgresql-common/pgdg \
# && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
# https://www.postgresql.org/media/keys/ACCC4CF8.asc \
# && . /etc/os-release \
# && printf '%s\n' \
# 'Types: deb' \
# 'URIs: https://apt.postgresql.org/pub/repos/apt' \
# "Suites: ${VERSION_CODENAME}-pgdg" \
# 'Components: main' \
# 'Signed-By: /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc' \
# > /etc/apt/sources.list.d/pgdg.sources \
# && rm -rf /var/lib/apt/lists/*

# RUN apt-get update -y && apt-get install -y \
#     build-essential \
#     ca-certificates \
#     wget \
#     curl \
#     flex \
#     bison \
#     autoconf \
#     automake \
#     libtool \
#     pkg-config \
#     libssl-dev \
#     libreadline-dev \
#     zlib1g-dev \
#     libpam0g-dev \
#     libxml2-dev \
#     libpq-dev \
#     libevent-dev \
#     libmemcached-dev \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /tmp
# RUN set -eux; \
#     curl -fL -o pgpool.tar.gz "https://github.com/pgpool/pgpool2/archive/refs/tags/${PGPOOL_TAG}.tar.gz"; \
#     tar tzf pgpool.tar.gz >/dev/null; \
#     src_dir="$(tar tzf pgpool.tar.gz | head -1 | cut -d/ -f1)"; \
#     tar xzf pgpool.tar.gz; \
#     cd "$src_dir"; \
#     if [ ! -x ./configure ]; then autoreconf -fi; fi; \
#     ./configure --prefix=/usr/local --with-openssl; \
#     make -j"$(nproc)"; \
#     make install

# FROM ${UBUNTU_IMAGE}

# ARG DEBIAN_FRONTEND=noninteractive
# ARG PG_MAJOR

# # HTTPS の PGDG に接続できるよう、CA 証明書を先に入れる
# RUN apt-get update && apt-get install -y ca-certificates \
# && rm -rf /var/lib/apt/lists/*

# # builder で作った PGDG の鍵とリポジトリ設定をコピーする
# COPY --from=builder /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
#                     /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
# COPY --from=builder /etc/apt/sources.list.d/pgdg.sources /etc/apt/sources.list.d/pgdg.sources

# RUN apt-get update -y && apt-get install -y \
#     ca-certificates \
#     tini \
#     procps \
#     libssl3t64 \
#     libreadline8t64 \
#     zlib1g \
#     libxml2 \
#     libpq5 \
#     libevent-2.1-7t64 \
#     libmemcached11t64 \
#     postgresql-client-${PG_MAJOR} \
#     && rm -rf /var/lib/apt/lists/*

# RUN groupadd -r -g 999 pgpool && \
#     useradd -u 999 -g pgpool -r -m -d /var/lib/pgpool -s /usr/sbin/nologin pgpool && \
#     mkdir -p /etc/pgpool2 /var/run/pgpool /var/log/pgpool && \
#     chown -R pgpool:pgpool /etc/pgpool2 /var/run/pgpool /var/log/pgpool /var/lib/pgpool

# COPY --from=builder /usr/local/ /usr/local/
# COPY ./pgpool/entrypoint.sh /usr/local/bin/entrypoint.sh
# RUN chmod +x /usr/local/bin/entrypoint.sh
# # COPY --chmod=755 ./entrypoint.sh ./start.sh /usr/local/bin/

# RUN ldconfig && \
#     chown -R pgpool:pgpool /usr/local/etc


# EXPOSE 5432 9898
# USER pgpool
# ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]

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

