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

    assertThat(outputDirectory.getChild("foo").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("bar").getChild("baz").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("bar").getChild("quux")).isNull();
  }

  @Test
  public void pathologicalSortingWithSubdirs() {
    // induce a list that would be mismatched sorted if the directory base were compared
    // against subdirectories
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of("a/file"),
        ImmutableList.<String>of("a+b", "a/b/c"));

    assertThat(outputDirectory.getChild("a").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("a+b").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("a").getChild("b").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("a").getChild("b").getChild("c").isLeaf()).isFalse();
  }

  @Test
  public void peerOutputFilesReduceToOneOutputDir() {
    // create two references to output directory 'bar'
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of("bar/baz", "bar/foo"),
        ImmutableList.<String>of());

    assertThat(outputDirectory.getChild("bar").isLeaf()).isTrue();
  }

  @Test
  public void recursiveOutputDirectoryIsRecursive() {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of(),
        ImmutableList.<String>of("foo"));

    assertThat(outputDirectory.getChild("foo").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("foo").getChild("bar").isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("foo").getChild("bar").getChild("baz").isLeaf()).isFalse();
    // and so on...
  }

  @Test
  public void emptyDirectoryIsRecursive() {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of(),
        ImmutableList.<String>of(""));

    assertThat(outputDirectory.isLeaf()).isFalse();
    assertThat(outputDirectory.getChild("foo").isLeaf()).isFalse();
  }

  @Test(expected=IllegalArgumentException.class)
  public void duplicateDirectorySeparatorIsInvalid() {
    OutputDirectory outputDirectory = OutputDirectory.parse(
        ImmutableList.<String>of(),
        ImmutableList.<String>of("a//b"));
  }
}
