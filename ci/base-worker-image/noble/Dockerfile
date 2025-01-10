# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:24.04@sha256:278628f08d4979fb9af9ead44277dbc9c92c2465922310916ad0c46ec9999295

RUN apt-get update
RUN apt-get -y install openjdk-21-jdk build-essential libfuse2 cgroup-tools
