# Many

This allows you to build a simple bazel C++ build graph of any size.
It consists of a toplevel filegroup named `cc`. This filegroup
contains `MANY_CC_BINARIES` program binaries, where `MANY_CC_BINARIES`
is an environment variable whose default value is `1`. Each of these
programs links with `MANY_CC_LIBRARIES`, each of which is generated
from `MANY_CC_LIBRARY_SOURCES`+1 files.

Simply set the environment variables as desired in order to build the
corresponding graph. For instance:

    MANY_CC_BINARIES=20 MANY_CC_LIBRARIES=10 MANY_CC_LIBRARY_SOURCES=5 bazel build //:cc
