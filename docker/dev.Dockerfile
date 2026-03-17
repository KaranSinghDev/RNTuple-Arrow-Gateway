# Tag list: https://hub.docker.com/r/rootproject/root/tags
FROM rootproject/root:6.36.00-ubuntu25.04

# Ubuntu 25.04 (plucky) ships Arrow in its default repos.
# No external PPA needed.
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
