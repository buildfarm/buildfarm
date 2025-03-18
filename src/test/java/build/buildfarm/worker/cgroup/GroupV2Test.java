// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.cgroup;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** These test CGroups v2 behavior exclusively. */
@RunWith(JUnit4.class)
public class GroupV2Test {

  @Before
  public void setup() {
    Group.VERSION = CGroupVersion.CGROUPS_V2;
  }

  @Test
  public void testHierarchy() {
    Group g = Group.getRoot().getChild("c1");
    assertThat(g).isNotNull();
    assertThat(g.getHierarchy()).isEqualTo("c1");
    assertThat(g.getChild("c2").getHierarchy()).isEqualTo("c1/c2");
  }

  @Test
  public void testGetPath() {
    Group g = Group.getRoot().getChild("c1");
    assertThat(g.getPath()).isEqualTo(Path.of("/sys/fs/cgroup/c1"));

    Group g2 = g.getChild("c2");
    assertThat(g2.getPath()).isEqualTo(Path.of("/sys/fs/cgroup/c1/c2"));
  }

  @Test
  public void testIsEmpty() throws IOException {
    Group mockGroup = spy(Group.getRoot().getChild("c1"));
    when(mockGroup.getPids()).thenReturn(Set.of(7, 8, 9));

    assertThat(mockGroup.isEmpty()).isFalse();
  }
}
