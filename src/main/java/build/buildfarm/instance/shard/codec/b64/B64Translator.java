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

package build.buildfarm.instance.shard.codec.b64;

import build.buildfarm.common.redis.StringTranslator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
class B64Translator<T extends Message> implements StringTranslator<T> {
  private static final Base64.Decoder DECODER = Base64.getDecoder();
  private static final Base64.Encoder ENCODER = Base64.getEncoder();
  private final Parser<T> parser;
  private final String name;

  B64Translator(Parser<T> parser, String name) {
    this.parser = parser;
    this.name = name;
  }

  @Override
  public T parse(String b64) {
    if (b64 != null) {
      try {
        return parser.parseFrom(DECODER.decode(b64));
      } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
        log.log(Level.SEVERE, "error parsing " + name + " from " + b64, e);
      }
    }
    return null;
  }

  @Override
  public String print(T value) {
    if (value != null) {
      ByteBuffer bb = value.toByteString().asReadOnlyByteBuffer();
      // per docs, equivalent of ENCODER.encodeToString(bb), hopefully proto bb always has backing
      // array
      return new String(ENCODER.encode(bb).array(), StandardCharsets.ISO_8859_1);
    }
    return null;
  }
}
