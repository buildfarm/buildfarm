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

package build.buildfarm.instance.shard.codec;

import static java.lang.String.format;

import build.buildfarm.common.redis.StringTranslator;
import java.time.Instant;
import lombok.extern.java.Log;

@Log
public class InstantTranslator implements StringTranslator<Instant> {
  @Override
  public String print(Instant instant) {
    return format("%d", instant.toEpochMilli());
  }

  @Override
  public Instant parse(String value) {
    if (value != null) {
      try {
        return Instant.ofEpochMilli(Long.parseLong(value));
      } catch (NumberFormatException e) {
        log.severe(format("invalid instant %s", value));
      }
    }
    return null;
  }
}
