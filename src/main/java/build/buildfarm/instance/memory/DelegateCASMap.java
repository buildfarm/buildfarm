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

package build.buildfarm.instance.memory;

import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import build.bazel.remote.execution.v2.Digest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class DelegateCASMap<K,V extends Message> {
  private final ContentAddressableStorage contentAddressableStorage;
  private final Parser<V> parser;
  private final DigestUtil digestUtil;
  private final Map<K, Digest> digestMap = new ConcurrentHashMap<>();

  public DelegateCASMap(
      ContentAddressableStorage contentAddressableStorage,
      Parser<V> parser,
      DigestUtil digestUtil) {
    this.contentAddressableStorage = contentAddressableStorage;
    this.parser = parser;
    this.digestUtil = digestUtil;
  }

  public V put(K key, V value) throws InterruptedException {
    Blob blob = new Blob(value.toByteString(), digestUtil);
    digestMap.put(key, blob.getDigest());
    contentAddressableStorage.put(blob, () -> digestMap.remove(key));
    return value;
  }

  public V get(Object key) {
    Digest valueDigest = digestMap.get(key);
    if (valueDigest == null) {
      return null;
    }

    return expectValueType(valueDigest);
  }

  public boolean containsKey(Object key) {
    return digestMap.get(key) != null;
  }

  public V remove(Object key) {
    Digest valueDigest = digestMap.remove(key);
    return expectValueType(valueDigest);
  }

  private V expectValueType(Digest valueDigest) {
    try {
      Blob blob = contentAddressableStorage.get(valueDigest);
      if (blob == null) {
        return null;
      }
      return parser.parseFrom(blob.getData());
    } catch (InvalidProtocolBufferException ex) {
      return null;
    }
  }
}
