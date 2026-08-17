FROM debian:bookworm-slim AS builder

ARG HIGHS_VERSION=1.15.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN curl -fsSL \
        "https://github.com/ERGO-Code/HiGHS/archive/refs/tags/v${HIGHS_VERSION}.tar.gz" \
        -o highs.tar.gz \
    && mkdir source \
    && tar -xzf highs.tar.gz --strip-components=1 -C source \
    && cmake -S source -B cmake-build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/highs \
        -DFAST_BUILD=ON \
        -DHIPO=OFF \
        -DBUILD_OPENBLAS=OFF \
        -DBUILD_STATIC_EXE=ON \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TESTING=OFF \
        -DBUILD_EXAMPLES=OFF \
    && cmake --build cmake-build --parallel \
    && cmake --install cmake-build \
    && /opt/highs/bin/highs --version

FROM debian:bookworm-slim AS runtime

ARG HIGHS_VERSION=1.15.1

LABEL org.opencontainers.image.title="HiGHS"
LABEL org.opencontainers.image.version="${HIGHS_VERSION}"
LABEL org.opencontainers.image.source="https://github.com/ERGO-Code/HiGHS"

COPY --from=builder /opt/highs/bin/highs /usr/local/bin/highs

WORKDIR /work

ENTRYPOINT ["highs"]
