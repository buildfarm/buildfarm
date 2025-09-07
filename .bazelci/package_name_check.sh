#!/bin/bash
# Copyright 2021-2025 The Buildfarm Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run this from the root of repository.
# The script will check that each java source file has the a package name that matches its file path.
# This is done for more than just consistency. Bazel's code coverage seems to exclude files when the package name does not match.

# Print an error such that it will surface in the context of buildkite.
print_error() {
    >&2 echo "$1"
    if [ -v BUILDKITE ] ; then
        buildkite-agent annotate "$1" --style 'error' --context 'ctx-error'  --append
    fi
}

extract_package_name_from_source() {
        local file=$1
        local package_name=`sed -n '/^package /p' $file | head -n 1`
        package_name=${package_name:8}
        package_name=${package_name::-1}
        echo $package_name
}
derive_package_name_from_file() {
        local package_name=$1

        # remove path prefix
        package_name=${package_name#"src/main/java/"}
        package_name=${package_name#"src/test/java/"}

        # remove file name
        package_name="${package_name%/*}"

        # turn path substring into package
        package_name="${package_name////$'.'}"
        echo $package_name
}

# Get all of the files to analyze.
files=$(find src/* -type f -name "*.java")

# Collect all the files whose package statements do not match expected format.
incorrect_files=()
for file in $files
do
        current_package_name=$(extract_package_name_from_source $file)
        expected_package_name=$(derive_package_name_from_file $file)
         if [ "$current_package_name" != "$expected_package_name" ]
	then
	      incorrect_files+=($file)
	fi
done


# If there are no package issues, we complete successfully.
if [ ${#incorrect_files[@]} -eq 0 ]; then
    echo "All files have expected package format.";
    exit 0;
fi;


# If there are package issues, we report them and fail overall.
print_error "There are files whose package statement does not match their file path:";
for value in "${incorrect_files[@]}"
do
     print_error $value
done
exit -1;