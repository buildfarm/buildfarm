"""
Provide a simple C++ build graph as large as desired.
"""

load("@bazel_skylib//rules:write_file.bzl", _write_file = "write_file")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

def write_file(name, out, lines):
    """
    Call the skylib write_file routine, ensuring that all lines end with a newline.
    """
    _write_file(
        name = name,
        out = out,
        content = lines + [""],
    )

def id(s):
    return s.replace("-", "_")

def many_cc_library_label(library_key):
    return "lib{}".format(library_key)

def many_cc_library_header(library_key):
    return "lib{}.hh".format(library_key)

def many_cc_library_function(library_key):
    return "func_{}".format(id(library_key))

def many_cc_library(library_key, library_source_count):
    """
    Generate a C++ library.

    The library has library_source_count+1 distinct functions.  One
    function is always present and is named for the library.  It calls
    all of the remaining functions in the library.

    Args:
        library_key: determines the name of the C++ library
        library_source_count: determines how many source files the library will have
    """
    library_source_index = 0
    cc_labels = []
    hh_files = []
    funcnames = []
    for library_source_index in range(library_source_count):
        rulename_hh = "lib{}-{}_hh".format(library_key, library_source_index)
        filename_hh = "lib{}-{}.hh".format(library_key, library_source_index)
        rulename_cc = "lib{}-{}_cc".format(library_key, library_source_index)
        filename_cc = "lib{}-{}.cc".format(library_key, library_source_index)
        funcname = "func_{}_{}".format(id(library_key), library_source_index)

        cc_labels.append(rulename_cc)
        cc_labels.append(rulename_hh)
        hh_files.append(filename_hh)
        funcnames.append(funcname)

        lines = []
        lines.append("#include <iostream>")
        lines.append('#include "{}"'.format(filename_hh))
        lines.append("void {}(void)".format(funcname))
        lines.append("{")
        lines.append('    std::cout << "Hello, World! {} {}" << std::endl;'.format(library_key, library_source_index))
        lines.append("}")

        write_file(
            name = rulename_cc,
            out = filename_cc,
            lines = lines,
        )

        lines = []
        lines.append("extern void {}(void);".format(funcname))

        write_file(
            name = rulename_hh,
            out = filename_hh,
            lines = lines,
        )

        library_source_index += 1

    rulename_hh = "lib{}_hh".format(library_key)
    filename_hh = many_cc_library_header(library_key)
    rulename_cc = "lib{}_cc".format(library_key)
    filename_cc = "lib{}.cc".format(library_key)
    funcname = many_cc_library_function(library_key)

    lines = []
    lines.extend(['#include "{}"'.format(x) for x in hh_files])
    lines.append('#include "{}"'.format(filename_hh))
    lines.append("void {}(void)".format(funcname))
    lines.append("{")
    lines.extend(["    {}();".format(x) for x in funcnames])
    lines.append("}")

    write_file(
        name = rulename_cc,
        out = filename_cc,
        lines = lines,
    )

    lines = []
    lines.append("extern void {}(void);".format(funcname))

    write_file(
        name = rulename_hh,
        out = filename_hh,
        lines = lines,
    )

    cc_library(
        name = many_cc_library_label(library_key),
        srcs = cc_labels + [rulename_cc],
        hdrs = [rulename_hh],
    )

def many_cc_binary_label(binary_key):
    return "{}".format(binary_key)

def many_cc_binary(binary_key, library_count, library_source_count):
    """
    Generate a C++ program.

    The program links with library_count libraries, each of which has
    library_source_count+1 functions.

    Args:
        binary_key: determines the name of the C++ program
        library_count: how many libraries this program should link with
        library_source_count: how many source files each library should have
    """
    library_keys = []
    for library_index in range(library_count):
        library_key = "{}-{}".format(binary_key, library_index)
        library_keys.append(library_key)
        many_cc_library(library_key, library_source_count)

    rulename_cc = "{}_cc".format(binary_key)
    filename_cc = "{}.cc".format(binary_key)

    lines = []
    lines.extend(['#include "{}"'.format(many_cc_library_header(x)) for x in library_keys])
    lines.append("int main(void)")
    lines.append("{")
    lines.extend(["    {}();".format(many_cc_library_function(x)) for x in library_keys])
    lines.append("    return 0;")
    lines.append("}")

    write_file(
        name = rulename_cc,
        out = filename_cc,
        lines = lines,
    )

    cc_binary(
        name = many_cc_binary_label(binary_key),
        srcs = [rulename_cc],
        deps = [many_cc_library_label(x) for x in library_keys],
    )

def many_cc(name, binary_count, library_count, library_source_count):
    """
    Generate a C++ build graph under a filegroup of the given name.

    The build graph has binary_count programs, each of which links with
    library_count libraries, each of which has library_source_count+1
    functions.

    Args:
        name: name of the filegroup to generate
        binary_count: how many programs to generate in the filegroup
        library_count: how many libraries each program should link with
        library_source_count: how many source files each library should have
    """
    binary_keys = []
    for binary_index in range(binary_count):
        binary_key = "{}-{}".format(name, binary_index)
        binary_keys.append(binary_key)
        many_cc_binary(binary_key, library_count, library_source_count)

    native.filegroup(
        name = name,
        srcs = [many_cc_binary_label(x) for x in binary_keys],
    )
