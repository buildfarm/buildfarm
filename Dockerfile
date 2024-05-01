# A minimal container for building and running buildfarm services.
# We copy in the current state of the reposiory to test against PR changes.
FROM ubuntu:24.04

# For network stability we allow apt-get to retry.
# The "80" is required for config priority but its not specifically important.
RUN echo 'APT::Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries

RUN apt update
RUN apt -y install wget zip git gcc openjdk-21-jre g++
COPY . buildfarm
