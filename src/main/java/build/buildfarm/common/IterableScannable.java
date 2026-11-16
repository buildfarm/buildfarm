// Copyright 2024-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common;

import static com.google.common.collect.Iterables.skip;

import java.util.function.Consumer;

public final class IterableScannable<T> implements Scannable<T> {
  private final String name;
  private final Iterable<T> iterable;

  public IterableScannable(String name, Iterable<T> iterable) {
    this.name = name;
    this.iterable = iterable;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String scan(int limit, String pageToken, Consumer<T> onItem) {
    if (limit <= 0) {
      return pageToken;
    }
    int index = pageToken.isEmpty() ? 0 : Integer.parseInt(pageToken);
    for (T item : skip(iterable, index)) {
      if (limit-- <= 0) {
        break;
      }
      index++;
      onItem.accept(item);
    }
    return limit < 0 ? Integer.toString(index) : SENTINEL_PAGE_TOKEN;
  }
}
