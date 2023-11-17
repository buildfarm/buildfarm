// Copyright 2023 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Command;
import build.buildfarm.worker.OutputDirectory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CFCExecFileSystemTest {
  @Test
  public void outputDirectoryWorkingDirectoryRelative() {
    Command command =
        Command.newBuilder()
            .setWorkingDirectory("foo/bar")
            .addOutputFiles("baz/quux")
            .addOutputDirectories("nope")
            .build();

    // verification is actually here with checked contents below
    // throws unless the directory is relative to the WorkingDirectory
    OutputDirectory workingOutputDirectory =
        CFCExecFileSystem.createOutputDirectory(command).getChild("foo").getChild("bar");
    assertThat(workingOutputDirectory.getChild("baz").isLeaf()).isTrue();
    assertThat(workingOutputDirectory.getChild("nope").isLeaf()).isFalse();
  }
}
