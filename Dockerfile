# A minimal container for building and running buildfarm services.
# We copy in the current state of the reposiory to test against PR changes.
FROM ubuntu:22.04

# For network stability we allow apt-get to retry.
# The "80" is required for config priority but its not specifically important.
RUN echo 'APT::Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries

RUN apt-get update
RUN apt-get -y install git zip python3 python3-pip gcc openjdk-21-jdk g++ redis redis-server

RUN pip3 install python-dateutil==2.9.0.post0
COPY . buildfarm
