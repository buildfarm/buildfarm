"""
buildfarm definitions that can be imported into other WORKSPACE files
"""

load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")

def buildfarm_init(name = "buildfarm"):
    """
    Initialize the WORKSPACE for buildfarm-related targets

    Args:
      name: the name of the repository
    """

    grpc_java_repositories()
