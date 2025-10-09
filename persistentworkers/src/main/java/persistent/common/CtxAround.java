// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
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

package persistent.common;

public interface CtxAround<T> {
  T get();

  // i.e. no context
  class Id<T> implements CtxAround<T> {
    T value;

    public Id(T value) {
      this.value = value;
    }

    @Override
    public T get() {
      return value;
    }

    public static <T> Id<T> of(T value) {
      return new Id<>(value);
    }

    @Override
    public boolean equals(Object obj) {
      return obj.equals(this.value);
    }
  }
}
