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

package build.buildfarm.common.config;

import static build.buildfarm.common.base.System.isWindows;

import build.buildfarm.v1test.WorkerType;
import com.google.common.base.Strings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.naming.ConfigurationException;
import lombok.Data;
import lombok.extern.java.Log;

@Data
@Log
public class Worker {
  private int port = 8981;
  private GrpcMetrics grpcMetrics = new GrpcMetrics();
  private String publicName;
  private Capabilities capabilities = new Capabilities();
  private String root = "/tmp/worker";
  private int inlineContentLimit = 1048567; // 1024 * 1024
  private long operationPollPeriod = 1;
  private DequeueMatchSettings dequeueMatchSettings = new DequeueMatchSettings();
  private List<Cas> storages = Arrays.asList(new Cas());
  private int executeStageWidth = 0;
  private int executeStageWidthOffset = 0;
  private int inputFetchStageWidth = 0;
  private int inputFetchDeadline = 60;
  private int reportResultStageWidth = 1;
  private boolean linkExecFileSystem = true;
  private boolean linkInputDirectories = true;
  private List<String> linkedInputDirectories = Arrays.asList("(?!external/)[^/]+");
  private String execOwner;
  private List<String> execOwners = new ArrayList<>();
  private int defaultMaxCores = 0;
  private boolean limitGlobalExecution = false;
  private boolean onlyMulticoreTests = false;
  private boolean allowBringYourOwnContainer = false;
  private boolean errorOperationRemainingResources = false;
  private int gracefulShutdownSeconds = 0;
  private List<ExecutionPolicy> executionPolicies = Collections.emptyList();
  private SandboxSettings sandboxSettings = new SandboxSettings();
  private boolean createSymlinkOutputs = false;
  private int zstdBufferPoolSize = 2048; /* * ZSTD_DStreamInSize (current is 128k) == 256MiB */
  private Set<String> persistentWorkerActionMnemonicAllowlist = Set.of("*");
  // These limited resources are only for the individual worker.
  // An example would be hardware resources such as GPUs.
  // If you want GPU actions to run exclusively, define a single GPU resource.
  private List<LimitedResource> resources = new ArrayList<>();

  private boolean errorOperationOutputSizeExceeded = false;
  private boolean legacyDirectoryFileCache = false;
  private boolean absolutizeCommandProgram = isWindows();

  public List<ExecutionPolicy> getExecutionPolicies() {
    return executionPolicies;
  }

  public int getWorkerType() {
    int workerType = 0;
    if (getCapabilities().isCas()) {
      workerType |= WorkerType.STORAGE.getNumber();
    }
    if (getCapabilities().isExecution()) {
      workerType |= WorkerType.EXECUTE.getNumber();
    }
    return workerType;
  }

  public Path getValidRoot() throws ConfigurationException {
    verifyRootConfiguration();
    addRootIfMissing();
    verifyRootLocation();
    return Path.of(root);
  }

  private void addRootIfMissing() throws ConfigurationException {
    try {
      if (!Files.isDirectory(Path.of(root))) {
        Files.createDirectories(Path.of(root));
      }
    } catch (Exception e) {
      throw new ConfigurationException(e.toString());
    }
  }

  private void verifyRootConfiguration() throws ConfigurationException {
    // Configuration error if no root is specified.
    if (Strings.isNullOrEmpty(root)) {
      throw new ConfigurationException("root value in config missing");
    }
  }

  private void verifyRootLocation() throws ConfigurationException {
    // Configuration error if root does not exist.
    if (!Files.isDirectory(Path.of(root))) {
      throw new ConfigurationException("root [" + root + "] is not directory");
    }
  }
}
