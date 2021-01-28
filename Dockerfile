FROM ubuntu:18.04
RUN apt-get update
RUN apt-get -y install wget python gcc openjdk-8-jdk g++

COPY . repo
RUN cd repo; ./bazelw test --test_output=errors -- //src/test/...;