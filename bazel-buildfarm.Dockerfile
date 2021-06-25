FROM openjdk:8u292-jdk-buster

# For network stability we allow apt-get to retry.
# The "80" is required for config priority but its not specifically important.
RUN echo 'APT::Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries

RUN apt-get update
RUN apt-get -y install wget git python gcc g++ redis redis-server make cmake
COPY . buildfarm
RUN mkdir -p /tmp/worker;
RUN cd buildfarm; ./bazelw build //src/main/java/build/buildfarm:buildfarm-server //src/main/java/build/buildfarm:buildfarm-shard-worker

