# Tag list: https://hub.docker.com/r/rootproject/root/tags
FROM rootproject/root:6.36.00-ubuntu25.04

# Build tools + Arrow's system deps (gRPC 1.51.1, abseil, RE2 all available in Ubuntu 25.04).
# Having these from apt means Arrow only compiles its own core — ~10-20 min vs ~60-90 min.
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    ninja-build \
    ccache \
    git \
    ca-certificates \
    libssl-dev \
    zlib1g-dev \
    liblz4-dev \
    libzstd-dev \
    libsnappy-dev \
    libgrpc++-dev \
    libabsl-dev \
    libre2-dev \
    protobuf-compiler-grpc \
    libprotobuf-dev \
    libgtest-dev \
    libbenchmark-dev \
    python3-dev \
    python3-pip \
    pybind11-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Build Apache Arrow C++ 19.0.0 + Flight from source.
# System gRPC/protobuf/abseil/RE2 are used — Arrow only compiles its own core.
# One-time cost: ~10-20 min. Layer is cached on subsequent docker builds.
ARG ARROW_VERSION=19.0.0
ARG JOBS=2
RUN git clone --depth 1 --branch apache-arrow-${ARROW_VERSION} \
      https://github.com/apache/arrow.git /tmp/arrow-src \
  && cmake -S /tmp/arrow-src/cpp -B /tmp/arrow-build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DARROW_FLIGHT=ON \
      -DARROW_BUILD_STATIC=OFF \
      -DARROW_BUILD_TESTS=OFF \
      -DARROW_BUILD_BENCHMARKS=OFF \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_SNAPPY=ON \
  && cmake --build /tmp/arrow-build --parallel "${JOBS}" \
  && cmake --install /tmp/arrow-build \
  && ldconfig \
  && rm -rf /tmp/arrow-src /tmp/arrow-build

# Python packages: pyarrow + benchmark deps (uproot, awkward, matplotlib, pandas, numpy)
RUN pip3 install --no-cache-dir --break-system-packages \
    "pyarrow==${ARROW_VERSION}" \
    pytest \
    uproot \
    awkward \
    matplotlib \
    pandas \
    numpy

WORKDIR /workspace
