workspace(name = "build_buildfarm")

maven_jar(
    name = "com_github_pcj_google_options",
    artifact = "com.github.pcj:google-options:jar:1.0.0",
    sha1 = "85d54fe6771e5ff0d54827b0a3315c3e12fdd0c7",
)

new_local_repository(
    name = "com_google_protobuf",
    build_file = "./third_party/protobuf/3.2.0/BUILD",
    path = "./third_party/protobuf/3.2.0/",
)

new_local_repository(
    name = "com_google_protobuf_java",
    build_file = "./third_party/protobuf/3.2.0/com_google_protobuf_java.BUILD",
    path = "./third_party/protobuf/3.2.0/",
)

new_local_repository(
    name = "googleapis",
    path = "./third_party/googleapis/",
    build_file = "./third_party/googleapis/BUILD",
)
