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

package build.buildfarm.instance.shard.codec.json;

import build.buildfarm.common.redis.StringTranslator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
abstract class JsonTranslator<T extends Message> implements StringTranslator<T> {
  protected final JsonFormat.Parser parser;
  protected final JsonFormat.Printer printer;
  private final String name;

  JsonTranslator(String name) {
    this(JsonFormat.parser().ignoringUnknownFields(), JsonFormat.printer(), name);
  }

  protected JsonTranslator(JsonFormat.TypeRegistry typeRegistry, String name) {
    this(
        JsonFormat.parser().usingTypeRegistry(typeRegistry).ignoringUnknownFields(),
        JsonFormat.printer().usingTypeRegistry(typeRegistry),
        name);
  }

  private JsonTranslator(JsonFormat.Parser parser, JsonFormat.Printer printer, String name) {
    this.parser = parser;
    this.printer = printer;
    this.name = name;
  }

  @Override
  public T parse(String json) {
    if (json != null) {
      try {
        Message.Builder message = builder();
        parser.merge(json, message);
        return (T) message.build();
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing " + name + " from " + json, e);
      }
    }
    return null;
  }

  @Override
  public String print(T value) {
    if (value != null) {
      try {
        return printer.print(value);
      } catch (InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing " + name + " from " + value, e);
      }
    }
    return null;
  }

  protected abstract Message.Builder builder();
}
