#!/usr/bin/env bash
# An Apple specific Apple execution wrapper - mainly to set SDKROOT and
# DEVELOPER_DIR.
# There is no enforcement of where Xcode.app is installed locally or remote so
# isn't possible to assume the remote path is identical to the local path.

# In "Apple" actions we will see the following ENV - but Bazel has to read the
# Xcode config to map this to DEVELOPER_DIR and SDKROOT.
# XCODE_VERSION_OVERRIDE=13.0.0.13A233
# APPLE_SDK_PLATFORM=iPhoneSimulator
# APPLE_SDK_VERSION_OVERRIDE=15.0
# PWD=/private/tmp/worker-macos/shard/operations/6d9822e7-29b8-451b-908e-41b09250536f

set -e

# When given a XCODE_VERSION_OVERRIDE locate the specific Xcode and export
if [[ -n "${XCODE_VERSION_OVERRIDE:-}" ]]; then
    # For DEVELOPER_DIR - we need to map from how the host is configured to this
    # path.
    OVERRIDE_PATH=""

    # Store the version file adjacent to the worker to avoid re-querying launchd
    # for a given version.
    CACHED_VERSION_FILE="$(dirname $(dirname "$PWD"))/$XCODE_VERSION_OVERRIDE.info"
    if [[ -f "$CACHED_VERSION_FILE" ]]; then
        OVERRIDE_PATH="$(cat $CACHED_VERSION_FILE)"
    else
        SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
        # Assuming the user has installed xcode-locator adjacent to the wrapper
        # https://github.com/bazelbuild/bazel/blob/master/tools/osx/BUILD
        # note: This program is highly tied to darwin
        if [[ -f "$SCRIPT_DIR/xcode-locator" ]]; then
            VERSION=$($SCRIPT_DIR/xcode-locator -v 2> /dev/null | grep ${XCODE_VERSION_OVERRIDE})
            VERSION_PARTS=(${VERSION//:/ })
            OVERRIDE_PATH="${VERSION_PARTS[2]}"
            echo "$OVERRIDE_PATH" > "$CACHED_VERSION_FILE"
        fi
    fi

    # Check in /Applications/Xcode-$VERSION - an existing convention or one you can
    # follow without using xcode-locator
    if [[ ! -n "${OVERRIDE_PATH}" ]]; then
        OVERRIDE_PATH="/Applications/Xcode-${XCODE_VERSION_OVERRIDE%.*}.app/Contents/Developer/"
    fi

    if [[ -d "$OVERRIDE_PATH" ]]; then
        # Success
        export DEVELOPER_DIR="$OVERRIDE_PATH"
    else
        # If the user didn't give a developer dir - then set it.
        # xcode-select is installed as part of Xcode.
        export DEVELOPER_DIR="$(xcode-select -p)"
        [[ -n "$DEVELOPER_DIR" ]] || (>&2 echo "error: missing xcode" && exit 1)
    fi
fi

# In Apple based compiling and linking tools often needs the SDKROOT. We can't
# naively export SDKROOT though. This code is simple and also requires
# APPLE_SDK_PLATFORM to be set to set the SDK. For now, remove all fallbacks.
if [[ ! -n "${SDKROOT:-}" ]] && [[ -n "${APPLE_SDK_PLATFORM:-}" ]]; then
    # If provided incorrectly - most code on Apple platforms will fail due to
    # nested and low level usage of this variable - in code and ways not obvious
    export SDKROOT="${DEVELOPER_DIR}/Platforms/${APPLE_SDK_PLATFORM}.platform/Developer/SDKs/${APPLE_SDK_PLATFORM}${APPLE_SDK_VERSION_OVERRIDE:-}.sdk"
fi

"$@"
