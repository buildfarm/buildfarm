# Buildfarm - Android AOSP Build Client Instructions

## Server Configuration

You can start a Buildfarm server on a single machine using the example configuration by running `examples/bf-run start`. This will allow you
to verify your client side configuration

## Build Machine Configuration

On your build machine you will need to set the following environment variables to tell the Android build to use the server;

```bash
# Enable RBE usage in the build
export USE_RBE=1
# Do not authenticate to the RBE server, as per the Buildfarm example configuration
export RBE_use_rpc_credentials=false
# The endpoint of the RBE server
export RBE_service={RBE_SERVER_IP_ADDRESS}:8980
# The instance named `main` is the default in the Buildfarm example configuration
export RBE_instance=main
# Do not try to authenticate to the RBE server
export RBE_service_no_auth=true
# Do not use TLS to communicate with the RBE server
export RBE_service_no_security=true
# The number of RBE jobs to run concurrently. This should be roughly the number of CPU cores on your RBE server
export NINJA_REMOTE_NUM_JOBS=8

# Enable RBE for the different build types
export RBE_ABI_DUMPER=1
export RBE_ABI_LINKER=1
export RBE_CLANG_TIDY=1
export RBE_CXX_LINKS=1
export RBE_D8=1
export RBE_JAVAC=1
export RBE_METALAVA=1
export RBE_R8=1
export RBE_SIGNAPK=1
export RBE_TURBINE=1
export RBE_ZIP=1

# Set the build strategy for different build types.
export RBE_ABI_DUMPER_EXEC_STRATEGY=remote_local_fallback
export RBE_ABI_LINKER_EXEC_STRATEGY=remote_local_fallback
export RBE_CLANG_TIDY_EXEC_STRATEGY=remote_local_fallback
export RBE_CXX_EXEC_STRATEGY=remote_local_fallback
export RBE_CXX_LINKS_EXEC_STRATEGY=remote_local_fallback
export RBE_D8_EXEC_STRATEGY=remote_local_fallback
export RBE_JAR_EXEC_STRATEGY=remote_local_fallback
export RBE_JAVAC_EXEC_STRATEGY=remote_local_fallback
export RBE_METALAVA_EXEC_STRATEGY=remote_local_fallback
export RBE_R8_EXEC_STRATEGY=remote_local_fallback
export RBE_SIGNAPK_EXEC_STRATEGY=remote_local_fallback
export RBE_TURBINE_EXEC_STRATEGY=remote_local_fallback
export RBE_ZIP_EXEC_STRATEGY=remote_local_fallback

```

## Build

Running a build is the same as a local build;

```bash
. build/envsetup.sh
lunch {LUNCH_TARGET}
m
```