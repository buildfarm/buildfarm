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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.buildfarm.common.DigestUtil.HashFunction;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import com.google.protobuf.TextFormat;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.v1test.BuildFarmServerConfig;

// Test that example config files can load properly
@RunWith(JUnit4.class)
public class ExampleConfigsTest {

  @Test
  public void workerConfig() throws IOException {
    
    // parse text into protobuf
    Path configPath = Paths.get("examples/worker.config.example");
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
        WorkerConfig.Builder builder = WorkerConfig.newBuilder();
        TextFormat.merge(new InputStreamReader(configInputStream), builder);
        builder.build();
    }
  }
  
  @Test
  public void serverConfig() throws IOException {
    
    // parse text into protobuf
    Path configPath = Paths.get("examples/server.config.example");
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
        BuildFarmServerConfig.Builder builder = BuildFarmServerConfig.newBuilder();
        TextFormat.merge(new InputStreamReader(configInputStream), builder);
        builder.build();
    }
  }
  
  @Test
  public void shardWorkerConfig() throws IOException {
    
    // parse text into protobuf
    Path configPath = Paths.get("examples/shard-worker.config.example");
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
        ShardWorkerConfig.Builder builder = ShardWorkerConfig.newBuilder();
        TextFormat.merge(new InputStreamReader(configInputStream), builder);
        builder.build();
    }
  }
  
  
}