# A minimal container for building a base worker image.
# Buildfarm public releases are build using this image as a starting point.
FROM ubuntu:24.04@sha256:73b999393d07f2bf65a40373778a44dc9a6d54ad7718d29247d8ff9413c4d6a8

RUN apt-get update
RUN apt-get -y install openjdk-21-jdk build-essential libfuse2 cgroup-tools
