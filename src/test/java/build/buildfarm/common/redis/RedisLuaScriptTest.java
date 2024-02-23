// Copyright 2020 The Bazel Authors. All rights reserved.
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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * @class RedisLuaScriptTest
 * @brief exercises the RedisLuaScript class
 * @details DBG_AVA
 */
@RunWith(JUnit4.class)
public class RedisLuaScriptTest {
  @Mock private Jedis redis;

  private String script = "abc";
  private String scriptSHA1 = "a9993e364706816aba3e25717850c26c9cd0d89d";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // DBG_AAV fixme
  // Function under test: removeFromDequeue
  // Reason for testing: removing returns false because the queue is empty and there is nothing to
  // remove
  // Failure explanation: the queue was either not empty, or an error occured while removing from an
  // empty queue
  @Test
  public void canConstruct() throws Exception {
    // ARRANGE

    // ACT
    RedisLuaScript luaScript = new RedisLuaScript(script);

    // ASSERT
    assertThat(luaScript.getScript()).contains(script);
    assertThat(luaScript.getDigest()).contains(scriptSHA1);
  }


 @Test
 public void canEvalWithKnownScript() throws Exception {
   // ARRANGE
   List<String> keys = List.of("key0", "key1");
   List<String> args = List.of("arg0", "arg1");
   RedisLuaScript luaScript = new RedisLuaScript(script);

   // ACT
   Object obj = luaScript.eval(redis, keys, args);

   // ASSERT
   verify(redis).evalsha(luaScript.getDigest(), keys, args);
   verify(redis, never()).eval(anyString(), anyList(), anyList());
 }

 @Test
 public void canEvalWithUnknownScript() throws Exception {
   // ARRANGE
   when(redis.evalsha(anyString(), anyList(), anyList())).thenThrow(new JedisNoScriptException(""));
   List<String> keys = List.of("key0", "key1");
   List<String> args = List.of("arg0", "arg1");
   RedisLuaScript luaScript = new RedisLuaScript(script);

   // ACT
   Object obj = luaScript.eval(redis, keys, args);

   // ASSERT
   verify(redis).evalsha(luaScript.getDigest(), keys, args);
   verify(redis).eval(script, keys, args);
 }

}
