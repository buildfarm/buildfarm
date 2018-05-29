// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import build.buildfarm.v1test.WorkerConfig;
import javax.naming.ConfigurationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OutputDirectoryTest {
  @Test
  public void outputDirectoryParsesOutputFiles() {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of("foo", "bar/baz"),
        ImmutableList.<String>of());

    assertThat(outputDirectory.getChild("foo")).isNull();
    assertThat(outputDirectory.getChild("bar").isLeaf()).isTrue();
  }

  @Test
  public void outputDirectoryParsesOutputDirs() {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of(),
        ImmutableList.<String>of("bar/baz", "foo"));

    assertThat(outputDirectory.getChild("foo").isLeaf()).isTrue();
    assertThat(outputDirectory.getChild("bar").getChild("baz").isLeaf()).isTrue();
  }
}
