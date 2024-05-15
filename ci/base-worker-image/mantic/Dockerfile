# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:mantic-20240427@sha256:565d62d2283a7cc4b3d759d9a97a5bfcebeb341166f9076a4df504f8f106cd54

RUN apt-get update
RUN apt-get -y install default-jre default-jdk build-essential libfuse2 cgroup-tools
