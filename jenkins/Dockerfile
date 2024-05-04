# Published under https://artifactory.bluel3.com/ui/repos/tree/General/docker-fne/bazel-buildfarm/docker-ci:ubuntu-18.04
# A minimal container for publishing buildfarm
FROM ubuntu:24.04

# Prevent interactive prompts during apt-get install
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install --no-install-recommends \
    wget \
    curl \
    git \
    zip \
    python3 \
    gcc \
    openjdk-11-jdk \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN  curl -fsSL https://get.docker.com -o get-docker.sh && sh ./get-docker.sh

RUN wget https://github.com/bazelbuild/bazelisk/releases/download/v1.26.0/bazelisk-linux-amd64 \
    && chmod +x bazelisk-linux-amd64 \
    && mv bazelisk-linux-amd64 /usr/local/bin/bazelisk

RUN /usr/local/bin/bazelisk

# Copy the entrypoint script and make it executable
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh


ENTRYPOINT ["docker-entrypoint.sh"]
