# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:lunar-20231128@sha256:5a828e28de105c3d7821c4442f0f5d1c52dc16acf4999d5f31a3bc0f03f06edd

RUN apt-get update
RUN apt-get -y install openjdk-21-jdk build-essential libfuse2 cgroup-tools
