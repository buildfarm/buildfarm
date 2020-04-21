// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.memory;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.verifyZeroInteractions;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class DelegateCASMapTest {
  @Mock private ContentAddressableStorage storage;

  private final DigestUtil digestUtil = new DigestUtil(HashFunction.SHA256);

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void emptyValuePutDoesNothingToCAS() throws InterruptedException {
    DelegateCASMap<String, ActionResult> map =
        new DelegateCASMap<>(storage, ActionResult.parser(), digestUtil);

    map.put("test", ActionResult.getDefaultInstance());

    verifyZeroInteractions(storage);
  }

  @Test
  public void emptyValueIsPresentInMap() throws InterruptedException {
    DelegateCASMap<String, ActionResult> map =
        new DelegateCASMap<>(storage, ActionResult.parser(), digestUtil);

    map.put("test", ActionResult.getDefaultInstance());

    assertThat(map.containsKey("test")).isTrue();
  }

  @Test
  public void emptyValueGetDoesNothingToCAS() throws InterruptedException {
    DelegateCASMap<String, ActionResult> map =
        new DelegateCASMap<>(storage, ActionResult.parser(), digestUtil);

    map.put("test", ActionResult.getDefaultInstance());
    ActionResult value = map.get("test");

    assertThat(value).isEqualTo(ActionResult.getDefaultInstance());
    verifyZeroInteractions(storage);
  }

  @Test
  public void emptyValueRemoveDoesNothingToCAS() throws InterruptedException {
    DelegateCASMap<String, ActionResult> map =
        new DelegateCASMap<>(storage, ActionResult.parser(), digestUtil);

    map.put("test", ActionResult.getDefaultInstance());
    map.remove("test");

    assertThat(map.containsKey("test")).isFalse();
    verifyZeroInteractions(storage);
  }
}
