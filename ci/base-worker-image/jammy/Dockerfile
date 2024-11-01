# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:24.04

RUN apt-get update
RUN apt-get -y install default-jre default-jdk build-essential libfuse2 cgroup-tools
