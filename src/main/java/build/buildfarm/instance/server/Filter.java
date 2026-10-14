// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.server;

import build.buildfarm.common.Scannable;
import com.google.common.base.Predicate;
import lombok.Data;

@Data
public final class Filter<T> {
  private final Iterable<Scannable<T>> bounds;
  private final Predicate<T> predicate;

  public Filter(Iterable<Scannable<T>> locations) {
    this(locations, o -> true);
  }

  public Filter(Iterable<Scannable<T>> bounds, Predicate<T> predicate) {
    this.bounds = bounds;
    this.predicate = predicate;
  }
}
