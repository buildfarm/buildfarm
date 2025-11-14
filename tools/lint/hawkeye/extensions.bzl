"""Hawkeye bzlmod extensions"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

HAWKEYE_COORDINATES = [
    {
        "name": "hawkeye-aarch64-apple-darwin",
        "url": "https://github.com/korandoru/hawkeye/releases/download/v6.3.0/hawkeye-aarch64-apple-darwin.tar.xz",
        "sha256": "377c5091b7c737882ab402967f85d8676fc4e5c8ac8cd6269ac4ae3ae1fb776b",
    },
    {
        "name": "hawkeye-x86_64-apple-darwin",
        "url": "https://github.com/korandoru/hawkeye/releases/download/v6.3.0/hawkeye-x86_64-apple-darwin.tar.xz",
        "sha256": "d83baec17b8181e10db92c192792de981fad69397d251c7003f66ef3fe60ec67",
    },
    {
        "name": "hawkeye-x86_64-unknown-linux-gnu",
        "url": "https://github.com/korandoru/hawkeye/releases/download/v6.3.0/hawkeye-x86_64-unknown-linux-gnu.tar.xz",
        "sha256": "0880773da630542ec3c2dbd8f7c4232986eb911ce45b0e1d83d4af11157c46b3",
    },
]

def _hawkeye_impl(_ctx):
    for coords in HAWKEYE_COORDINATES:
        http_archive(
            name = coords["name"],
            build_file = "//tools/lint/hawkeye:hawkeye.BUILD",
            sha256 = coords["sha256"],
            strip_prefix = coords["name"],
            url = coords["url"],
        )

hawkeye = module_extension(implementation = _hawkeye_impl)
