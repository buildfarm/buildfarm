# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:22.04@sha256:9abcaffc92a5279fff3a6a41635c0800ea33e528425fb389658806476e7defde

RUN apt-get update
RUN apt-get -y install default-jre default-jdk build-essential libfuse2 cgroup-tools
