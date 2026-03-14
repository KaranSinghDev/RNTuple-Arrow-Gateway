# Verify tag at: https://hub.docker.com/r/rootproject/root/tags
FROM rootproject/root:6.36.00-ubuntu24.04

# Apache Arrow apt repository (Ubuntu 24.04 "noble")
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget \
  && wget -q https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb \
  && apt-get install -y --no-install-recommends \
    ./apache-arrow-apt-source-latest-noble.deb \
  && apt-get update \
  && rm ./apache-arrow-apt-source-latest-noble.deb \
  && rm -rf /var/lib/apt/lists/*

# Build tools + RAG dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    ninja-build \
    ccache \
    libarrow-dev \
    libarrow-flight-dev \
    libgtest-dev \
    libbenchmark-dev \
    python3-dev \
    python3-pip \
    pybind11-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Python packages needed from M2 onwards
RUN pip3 install --no-cache-dir --break-system-packages pyarrow

WORKDIR /workspace
