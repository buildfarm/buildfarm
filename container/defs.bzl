"""Rules for multi-architecture container images"""

load("@rules_img//img:image.bzl", "image_index")
load("@rules_img//img:push.bzl", "image_push")

def multiarch_oci_image(name, image):
    # For rules_img, we use image_index with platform transitions
    # The image_index rule will build the same manifest for multiple platforms
    image_index(
        name = name,
        manifests = [image],
        platforms = [
            "@hermetic_cc_toolchain//toolchain/platform:linux_amd64",
            "@hermetic_cc_toolchain//toolchain/platform:linux_arm64",
        ],
        tags = ["container"],
    )

    # Below targets push public docker images to bazelbuild dockerhub.
    image_push(
        name = "public_push_%s" % name,
        image = name,
        repository = "index.docker.io/bazelbuild/%s" % name,
        # Specify the tag with `bazel run public_push_buildfarm-server public_push_buildfarm-worker -- --tag latest`
        tags = ["container"],
    )
