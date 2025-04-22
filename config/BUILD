config_setting(
    name = "windows",
    constraint_values = ["@platforms//os:windows"],
    visibility = ["//visibility:public"],
)

config_setting(
    name = "open_telemetry",
    values = {"define": "open_telemetry=true"},
)

filegroup(
    name = "base_properties",
    srcs = glob(["*.properties"]),
    visibility = ["//visibility:public"],
)
