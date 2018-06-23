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

import build.buildfarm.common.ContentAddressableStorage;
import build.buildfarm.common.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class DelegateCASMap<K,V extends Message> implements Map<K,V> {
  private final ContentAddressableStorage contentAddressableStorage;
  private final Parser<V> parser;
  private final DigestUtil digestUtil;
  private final Map<K, Digest> digestMap = new ConcurrentHashMap<>();
  private final Cache<K, Digest> emptyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build();

  public DelegateCASMap(
      ContentAddressableStorage contentAddressableStorage,
      Parser<V> parser,
      DigestUtil digestUtil) {
    this.contentAddressableStorage = contentAddressableStorage;
    this.parser = parser;
    this.digestUtil = digestUtil;
  }

  @Override
  public V put(K key, V value) {
    Blob blob = new Blob(value.toByteString(), digestUtil);
    if (blob.size() == 0) {
      emptyCache.put(key, blob.getDigest());
    } else {
      digestMap.put(key, blob.getDigest());
      contentAddressableStorage.put(blob, () -> digestMap.remove(key));
    }
    return value;
  }

  @Override
  public V get(Object key) {
    Digest valueDigest = digestMap.get(key);
    if (valueDigest == null) {
      valueDigest = emptyCache.getIfPresent(key);
    }
    if (valueDigest == null) {
      return null;
    }

    return expectValueType(valueDigest);
  }

  @Override
  public boolean isEmpty() {
    return digestMap.isEmpty();
  }

  @Override
  public int size() {
    return digestMap.size();
  }

  @Override
  public boolean containsKey(Object key) {
    return digestMap.containsKey(key) || emptyCache.getIfPresent(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    Preconditions.checkState(value instanceof Message);
    return contentAddressableStorage.contains(digestUtil.compute((Message) value));
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return delegate().entrySet();
  }

  @Override
  public Collection<V> values() {
    return delegate().values();
  }

  @Override
  public Set<K> keySet() {
    return digestMap.keySet();
  }

  @Override
  public void clear() {
    digestMap.clear();
  }

  @Override
  public void putAll(Map<? extends K,? extends V> m) {
    Map<? extends K, Blob> blobs = Maps.transformValues(
        m,
        (value) -> new Blob(value.toByteString(), digestUtil));
    for (Blob blob : blobs.values()) {
      contentAddressableStorage.put(blob);
    }
    digestMap.putAll(Maps.transformValues(blobs, (blob) -> blob.getDigest()));
  }

  @Override
  public V remove(Object key) {
    Digest valueDigest = digestMap.remove(key);
    if (valueDigest == null) {
      emptyCache.invalidate(key);
      valueDigest = digestUtil.empty();
    }
    return expectValueType(valueDigest);
  }

  private V expectValueType(Digest valueDigest) {
    try {
      if (valueDigest.getSizeBytes() == 0) {
        return parser.parseFrom(ByteString.EMPTY);
      }

      Blob blob = contentAddressableStorage.get(valueDigest);
      if (blob == null) {
        return null;
      }
      return parser.parseFrom(blob.getData());
    } catch (InvalidProtocolBufferException ex) {
      return null;
    }
  }

  private Map<K, V> delegate() {
    return Maps.transformValues(
        digestMap,
        (valueDigest) -> expectValueType(valueDigest));
  }
}
