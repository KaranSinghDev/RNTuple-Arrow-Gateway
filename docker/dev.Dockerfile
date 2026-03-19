# Tag list: https://hub.docker.com/r/rootproject/root/tags
FROM rootproject/root:6.36.00-ubuntu25.04

# Build tools + compression libs + GTest + Benchmark + pybind11.
# Arrow is NOT in Ubuntu 25.04 repos, so we build it below.
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
    libgtest-dev \
    libbenchmark-dev \
    python3-dev \
    python3-pip \
    pybind11-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Build Apache Arrow C++ 19.0.0 + Flight from source.
# ARROW_DEPENDENCY_SOURCE defaults to AUTO: uses system libs where available,
# bundles (downloads + compiles) the rest — notably gRPC and protobuf.
# One-time cost: ~25-40 min. Layer is cached on subsequent docker builds.
ARG ARROW_VERSION=19.0.0
# Limit parallel jobs to control RAM use. Pass --build-arg JOBS=1 for ~1 GB peak.
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
      -DgRPC_SOURCE=BUNDLED \
      -DProtobuf_SOURCE=BUNDLED \
  && cmake --build /tmp/arrow-build --parallel "${JOBS}" \
  && cmake --install /tmp/arrow-build \
  && ldconfig \
  && rm -rf /tmp/arrow-src /tmp/arrow-build

# Python package pinned to match the Arrow C++ version above
RUN pip3 install --no-cache-dir --break-system-packages "pyarrow==${ARROW_VERSION}"

WORKDIR /workspace
