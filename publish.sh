#!/usr/bin/env bash
set -o errexit

# = Summary =
# This script produces buildfarm images suitable for av repo.
# Run the script, and new images will be produced and uploaded to artifactory.
# These images are later used in the kubernetes repo to deploy k8s pods.
# https://github.bluel3.com/cloud/kubernetes

# = Context =
# Buildfarm workers require some kind of base image to build the java_image used for deployment.
# In the case of AV, a build container is required for actions to pass successfully.

# = Versioning =
# Update the IMAGE_RELEASE tag in the BUILD file to produce images with different base versions.
# Update the VERSION var here to provide steppings for incremental activity.

VERSION=alpha.0

usage() {
    echo "Usage: $(basename "$0") [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -t <type>, --type <type>"
    echo "              Set the base version type (e.g., '10'). Defaults to 7."
    echo "              Can also use --type=<type> format."
    echo "  -c, --ci    Indicate that the script is running in a CI environment (e.g., Jenkins)."
    echo "  -h, --help  Display this help message."
    echo ""
    exit 1
}

opt_type=""
opt_in_ci=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--type)
            # Check if the argument for -t/--type is provided
            if [[ -z "$2" || "$2" == -* ]]; then
                echo "Error: Option $1 requires an argument." >&2
                usage
            fi
            opt_type="$2"
            shift 2 # Consume the option and its argument
            ;;
        --type=*)
            # Handle --type=value format
            opt_type="${1#--type=}" # Extract the value after '='
            shift # Consume the option
            ;;
        -c|--ci)
            opt_in_ci=true
            shift # Consume the option
            ;;
        -h|--help)
            usage
            ;;
        --) # End of options
            shift
            break
            ;;
        -*) # Unknown option
            echo "Error: Unknown option: $1" >&2
            usage
            ;;
        *) # Unexpected argument (if we don't expect any non-option args)
            echo "Error: Unexpected argument: $1" >&2
            usage
            ;;
    esac
done

shift "$((OPTIND - 1))"

if [ -n "$opt_type" ]; then
    VERSION="$opt_type"
    echo "Using specified base version: $VERSION"
else
    echo "Using default base version: $VERSION"
fi

if "$opt_in_ci"; then
    echo "Running in CI environment."
    BAZEL_CMD=./jenkins/docker-bazelisk.sh
else
    echo "Running in local environment."
    BAZEL_CMD=bazelisk
fi

echo "Using Bazel command: $BAZEL_CMD"
echo "Build and push images with version label: $VERSION"
echo ""

$BAZEL_CMD clean --expunge

$BAZEL_CMD run --embed_label "$VERSION" --stamp //container:push-buildfarm-server

$BAZEL_CMD run --embed_label "$VERSION" --stamp //container:push-buildfarm-worker
