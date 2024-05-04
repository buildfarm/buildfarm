#!/usr/bin/env bash

set -mo errexit

DOCKER_CI_IMAGE=artifactory.bluel3.com/docker-fne/bazel-buildfarm/docker-ci:bazel-jdk11
DOCKER_HOME_IN_CONTAINER=/root

TEMP_CONFIG_DIR=$(mktemp -d -t docker-publish-configs-XXXXXX)
echo "INFO: Using temporary config directory: $TEMP_CONFIG_DIR"

cleanup() {
    echo "INFO: Cleaning up temporary config directory: $TEMP_CONFIG_DIR"
    rm -rf "$TEMP_CONFIG_DIR"
}
trap cleanup EXIT HUP INT QUIT TERM

# --- Prepare .docker directory ---
if [ -d "$HOME/.docker" ]; then
    mkdir -p "$TEMP_CONFIG_DIR/.docker"
    cp -a "$HOME/.docker/." "$TEMP_CONFIG_DIR/.docker/"
    find "$TEMP_CONFIG_DIR/.docker" -type f -exec chmod 604 {} \;
    find "$TEMP_CONFIG_DIR/.docker" -type d -exec chmod 705 {} \; # d=rwx,g=---,o=r-x
    echo "INFO: Copied .docker directory to temporary location and adjusted permissions."
else
    echo "WARNING: $HOME/.docker directory not found. Proceeding without it."
    mkdir -p "$TEMP_CONFIG_DIR/.docker"
    chmod 705 "$TEMP_CONFIG_DIR/.docker"
fi

docker run --rm --name publish-buildfarm-bazel \
            --volume "/var/run/docker.sock:/var/run/docker.sock" \
            --volume "$TEMP_CONFIG_DIR/.docker:$DOCKER_HOME_IN_CONTAINER/.docker:ro" \
            --volume "$PWD:/src/workspace" \
            --workdir /src/workspace \
            "$DOCKER_CI_IMAGE" "$@"
