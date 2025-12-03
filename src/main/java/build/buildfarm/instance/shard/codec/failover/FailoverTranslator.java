// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.instance.shard.codec.failover;

import build.buildfarm.common.redis.StringTranslator;
import com.google.protobuf.Message;

class FailoverTranslator<T extends Message> implements StringTranslator<T> {
  private final StringTranslator<T> initial;
  private final StringTranslator<T> next;

  FailoverTranslator(StringTranslator<T> initial, StringTranslator<T> next) {
    this.initial = initial;
    this.next = next;
  }

  @Override
  public T parse(String value) {
    T t = null;
    if (value != null) {
      t = initial.parse(value);
      if (t == null) {
        t = next.parse(value);
      }
    }
    return t;
  }

  @Override
  public String print(T t) {
    return initial.print(t);
  }
}
