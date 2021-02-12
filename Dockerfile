# A minimal container for building and running buildfarm services.
# We copy in the current state of the reposiory to test against PR changes.
FROM ubuntu:18.04
RUN apt-get update
RUN apt-get -y install wget python gcc openjdk-8-jdk g++ redis redis-server
COPY . buildfarm
RUN mkdir -p /tmp/worker;
RUN cd buildfarm; ./bazelw build //src/main/java/build/buildfarm:buildfarm-server //src/main/java/build/buildfarm:buildfarm-shard-worker