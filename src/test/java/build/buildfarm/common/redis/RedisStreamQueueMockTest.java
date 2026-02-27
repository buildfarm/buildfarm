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

package build.buildfarm.common.redis;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.Visitor;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;
import redis.clients.jedis.resps.StreamPendingSummary;

@RunWith(JUnit4.class)
public class RedisStreamQueueMockTest {
  @Mock private Jedis redis;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    RedisStreamQueue.clearPendingEntries();
  }

  @After
  public void tearDown() {
    RedisStreamQueue.clearPendingEntries();
  }

  @Test
  public void decorateSucceeds() {
    RedisStreamQueue.decorate(redis, "test");
  }

  @Test
  public void offerShouldXAdd() {
    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    queue.offer("foo");

    ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(redis, times(1)).xadd(eq("test"), any(XAddParams.class), fieldsCaptor.capture());
    assertThat(fieldsCaptor.getValue()).containsEntry("v", "foo");
  }

  @Test
  public void offerWithPriorityShouldXAdd() {
    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    queue.offer("bar", 5.0);

    ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(redis, times(1)).xadd(eq("test"), any(XAddParams.class), fieldsCaptor.capture());
    assertThat(fieldsCaptor.getValue()).containsEntry("v", "bar");
  }

  @Test
  public void takeShouldXReadGroup() throws InterruptedException {
    // Arrange
    arrangeGroupExists();
    StreamEntryID entryId = new StreamEntryID(1000, 0);
    Map<String, String> fields = new HashMap<>();
    fields.put("v", "myvalue");
    StreamEntry entry = new StreamEntry(entryId, fields);
    List<Map.Entry<String, List<StreamEntry>>> result =
        ImmutableList.of(new AbstractMap.SimpleEntry<>("test", ImmutableList.of(entry)));
    when(redis.xreadGroup(
            eq(RedisStreamQueue.GROUP_NAME),
            anyString(),
            any(XReadGroupParams.class),
            any(Map.class)))
        .thenReturn(result);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    String value = queue.take(Duration.ofSeconds(1));

    // Assert
    assertThat(value).isEqualTo("myvalue");
    verify(redis, times(1))
        .xreadGroup(
            eq(RedisStreamQueue.GROUP_NAME),
            anyString(),
            any(XReadGroupParams.class),
            any(Map.class));
  }

  @Test
  public void takeReturnsNullWhenEmpty() throws InterruptedException {
    // Arrange
    arrangeGroupExists();
    when(redis.xreadGroup(anyString(), anyString(), any(XReadGroupParams.class), any(Map.class)))
        .thenReturn(null);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    String value = queue.take(Duration.ofSeconds(1));

    // Assert
    assertThat(value).isNull();
  }

  @Test
  public void pollShouldXReadGroupNonBlocking() {
    // Arrange
    arrangeGroupExists();
    StreamEntryID entryId = new StreamEntryID(2000, 0);
    Map<String, String> fields = new HashMap<>();
    fields.put("v", "polled");
    StreamEntry entry = new StreamEntry(entryId, fields);
    List<Map.Entry<String, List<StreamEntry>>> result =
        ImmutableList.of(new AbstractMap.SimpleEntry<>("test", ImmutableList.of(entry)));
    when(redis.xreadGroup(anyString(), anyString(), any(XReadGroupParams.class), any(Map.class)))
        .thenReturn(result);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    String value = queue.poll();

    // Assert
    assertThat(value).isEqualTo("polled");
  }

  @Test
  public void pollReturnsNullWhenEmpty() {
    // Arrange
    arrangeGroupExists();
    when(redis.xreadGroup(anyString(), anyString(), any(XReadGroupParams.class), any(Map.class)))
        .thenReturn(null);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    String value = queue.poll();

    // Assert
    assertThat(value).isNull();
  }

  @Test
  public void removeFromDequeueFastPathUsesXAck() throws InterruptedException {
    // Arrange: take a value first to populate the pending entries map
    arrangeGroupExists();
    StreamEntryID entryId = new StreamEntryID(3000, 0);
    Map<String, String> fields = new HashMap<>();
    fields.put("v", "toremove");
    StreamEntry entry = new StreamEntry(entryId, fields);
    List<Map.Entry<String, List<StreamEntry>>> result =
        ImmutableList.of(new AbstractMap.SimpleEntry<>("test", ImmutableList.of(entry)));
    when(redis.xreadGroup(anyString(), anyString(), any(XReadGroupParams.class), any(Map.class)))
        .thenReturn(result);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");
    queue.take(Duration.ofSeconds(1));

    // Act
    boolean removed = queue.removeFromDequeue("toremove");

    // Assert
    assertThat(removed).isTrue();
    verify(redis, times(1)).xack("test", RedisStreamQueue.GROUP_NAME, entryId);
    verify(redis, times(1)).xdel("test", entryId);
  }

  @Test
  public void removeFromDequeueSlowPathUsesLuaScript() {
    // Arrange: no prior take, so in-memory map is empty
    when(redis.eval(anyString(), any(List.class), any(List.class))).thenReturn(1L);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    boolean removed = queue.removeFromDequeue("unknown_value");

    // Assert
    assertThat(removed).isTrue();
    verify(redis, times(1)).eval(anyString(), any(List.class), any(List.class));
    // Should NOT use xack directly
    verify(redis, never()).xack(anyString(), anyString(), any(StreamEntryID.class));
  }

  @Test
  public void removeFromDequeueFalseWhenNotFound() {
    // Arrange: Lua script returns 0 (not found)
    when(redis.eval(anyString(), any(List.class), any(List.class))).thenReturn(0L);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    boolean removed = queue.removeFromDequeue("nonexistent");

    // Assert
    assertThat(removed).isFalse();
  }

  @Test
  public void sizeReturnsXLenMinusPending() {
    // Arrange
    when(redis.xlen("test")).thenReturn(10L);
    StreamPendingSummary summary = mock(StreamPendingSummary.class);
    when(summary.getTotal()).thenReturn(3L);
    when(redis.xpending("test", RedisStreamQueue.GROUP_NAME)).thenReturn(summary);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    long size = queue.size();

    // Assert
    assertThat(size).isEqualTo(7);
  }

  @Test
  public void sizeReturnsXLenWhenGroupNotExists() {
    // Arrange
    when(redis.xlen("test")).thenReturn(5L);
    when(redis.xpending("test", RedisStreamQueue.GROUP_NAME))
        .thenThrow(new JedisDataException("NOGROUP"));

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    long size = queue.size();

    // Assert
    assertThat(size).isEqualTo(5);
  }

  @Test
  public void visitShouldIterateStreamEntries() {
    // Arrange
    StreamEntryID id1 = new StreamEntryID(1000, 0);
    StreamEntryID id2 = new StreamEntryID(2000, 0);
    Map<String, String> fields1 = new HashMap<>();
    fields1.put("v", "first");
    Map<String, String> fields2 = new HashMap<>();
    fields2.put("v", "second");
    StreamEntry entry1 = new StreamEntry(id1, fields1);
    StreamEntry entry2 = new StreamEntry(id2, fields2);

    when(redis.xrange(eq("test"), any(StreamEntryID.class), any(), anyInt()))
        .thenReturn(ImmutableList.of(entry1, entry2));

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");
    Visitor<String> visitor = mock(Visitor.class);

    // Act
    queue.visit(visitor);

    // Assert
    verify(visitor, times(1)).visit("first");
    verify(visitor, times(1)).visit("second");
  }

  @Test
  public void visitDequeueShouldIteratePendingEntries() {
    // Arrange
    StreamEntryID id1 = new StreamEntryID(1000, 0);
    StreamPendingEntry pe1 = mock(StreamPendingEntry.class);
    when(pe1.getID()).thenReturn(id1);

    when(redis.xpending(eq("test"), eq(RedisStreamQueue.GROUP_NAME), any(XPendingParams.class)))
        .thenReturn(ImmutableList.of(pe1));

    Map<String, String> fields1 = new HashMap<>();
    fields1.put("v", "pending_value");
    StreamEntry entry1 = new StreamEntry(id1, fields1);
    when(redis.xrange(eq("test"), eq(id1), eq(id1), eq(1))).thenReturn(ImmutableList.of(entry1));

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");
    Visitor<String> visitor = mock(Visitor.class);

    // Act
    queue.visitDequeue(visitor);

    // Assert
    verify(visitor, times(1)).visit("pending_value");
  }

  @Test
  public void getDequeueName() {
    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");
    assertThat(queue.getDequeueName()).isEqualTo("test_pending");
  }

  @Test
  public void ensureGroupExistsHandlesBusyGroup() {
    // Arrange: simulate group already exists
    doThrow(new JedisDataException("BUSYGROUP Consumer Group name already exists"))
        .when(redis)
        .xgroupCreate(anyString(), anyString(), any(StreamEntryID.class), eq(true));
    when(redis.xreadGroup(anyString(), anyString(), any(XReadGroupParams.class), any(Map.class)))
        .thenReturn(null);

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act - should not throw
    queue.poll();
  }

  @Test
  public void scanReturnsEntriesWithCursor() {
    // Arrange
    StreamEntryID id1 = new StreamEntryID(1000, 0);
    StreamEntryID id2 = new StreamEntryID(2000, 0);
    Map<String, String> fields1 = new HashMap<>();
    fields1.put("v", "val1");
    Map<String, String> fields2 = new HashMap<>();
    fields2.put("v", "val2");

    when(redis.xrange(eq("test"), any(StreamEntryID.class), any(), eq(2)))
        .thenReturn(ImmutableList.of(new StreamEntry(id1, fields1), new StreamEntry(id2, fields2)));

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    redis.clients.jedis.resps.ScanResult<String> result = queue.scan("", 2, "*");

    // Assert
    assertThat(result.getResult()).containsExactly("val1", "val2");
    // Cursor should advance past the last entry
    assertThat(result.getCursor()).isNotEqualTo("0");
  }

  @Test
  public void scanReturnsEmptyAtEnd() {
    // Arrange
    when(redis.xrange(eq("test"), any(StreamEntryID.class), any(), anyInt()))
        .thenReturn(ImmutableList.of());

    RedisStreamQueue queue = new RedisStreamQueue(redis, "test");

    // Act
    redis.clients.jedis.resps.ScanResult<String> result = queue.scan("", 10, "*");

    // Assert
    assertThat(result.getResult()).isEmpty();
    assertThat(result.getCursor()).isEqualTo("0");
  }

  /** Arrange for xgroupCreate to succeed (group doesn't exist yet). */
  private void arrangeGroupExists() {
    when(redis.xgroupCreate(anyString(), anyString(), any(StreamEntryID.class), eq(true)))
        .thenReturn("OK");
  }
}
