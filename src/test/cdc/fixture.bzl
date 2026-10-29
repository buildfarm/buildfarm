# Copyright 2026 The Buildfarm Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A single action with no compiler, runtime, or shell toolchain dependencies."""

def _cdc_fixture_impl(ctx):
    output = ctx.actions.declare_file("artifact.bin")

    # cat is supplied by the Linux CI worker. Prepending a marker changes the
    # digest without depending on a compiler or a large executable to cause CDC.
    ctx.actions.run_shell(
        inputs = [ctx.file.src],
        outputs = [output],
        arguments = [ctx.file.src.path, output.path],
        command = "{ printf 'buildfarm-cdc-output\\n'; /bin/cat \"$1\"; } > \"$2\"",
        mnemonic = "CdcFixture",
        progress_message = "Transforming deterministic CDC fixture",
    )
    return [DefaultInfo(files = depset([output]))]

cdc_fixture = rule(
    implementation = _cdc_fixture_impl,
    attrs = {"src": attr.label(allow_single_file = True, mandatory = True)},
)
