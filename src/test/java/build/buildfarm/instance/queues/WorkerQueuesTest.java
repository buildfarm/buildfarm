// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.instance.queues;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.longrunning.Operation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkerQueuesTest {

  @Test
  public void noConfig_zeroQueues() {

    // Arrange
    WorkerQueues queues = new WorkerQueues();

    // Assert
    assertThat(queues.specificQueues).isEmpty();
  }

  @Test
  public void noConfig_emptyProvisionNotEnqueued() {

    // Arrange
    WorkerQueues queues = new WorkerQueues();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isFalse();
  }

  @Test
  public void noConfig_nullProvisionNotEnqueued() {

    // Arrange
    WorkerQueues queues = new WorkerQueues();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = null;
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isFalse();
  }

  @Test
  public void noConfig_invalidProvisionNotEnqueued() {

    // Arrange
    WorkerQueues queues = new WorkerQueues();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("invalid_key", "invalid_value");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isFalse();
  }

  @Test
  public void cpuGpuConfig_twoQueues() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpuAndFallback();

    // Assert
    assertThat(queues.specificQueues).hasSize(2);
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
    assertThat(queues.queueSize("Other")).isEqualTo(0);
  }

  @Test
  public void cpuGpuConfig_emptyProvisionEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpuAndFallback();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
    assertThat(queues.queueSize("Other")).isEqualTo(1);
  }

  @Test
  public void cpuGpuConfig_nullProvisionEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpuAndFallback();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = null;
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
    assertThat(queues.queueSize("Other")).isEqualTo(1);
  }

  @Test
  public void cpuGpuConfig_invalidProvisionEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpuAndFallback();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("invalid_key", "invalid_value");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
    assertThat(queues.queueSize("Other")).isEqualTo(1);
  }

  @Test
  public void cpuGpuConfig_validGpuProvisionEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpuAndFallback();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("gpu_required", "only key matters");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(1);
    assertThat(queues.queueSize("Other")).isEqualTo(0);
  }

  @Test
  public void gpuConfig_emptyProvisionNotEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpu();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isFalse();
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
  }

  @Test
  public void gpuConfig_validProvisionEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpu();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("gpu_required", "only key matters");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(1);
  }

  @Test
  public void gpuConfig_validProvisionsEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpu();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("foo", "bar");
    provisions.put("gpu_required", "only key matters");
    provisions.put("another", "provision");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isTrue();
    assertThat(queues.queueSize("GPU")).isEqualTo(1);
  }

  @Test
  public void gpuConfig_unvalidProvisionNotEnqueued() {

    // Arrange
    WorkerQueues queues = WorkerQueueConfigurations.gpu();

    // Act
    Operation operation = Operation.newBuilder().build();
    SetMultimap<String, String> provisions = HashMultimap.create();
    provisions.put("invalid_key", "invalid_value");
    boolean success = queues.enqueueOperation(operation, provisions);

    // Assert
    assertThat(success).isFalse();
    assertThat(queues.queueSize("GPU")).isEqualTo(0);
  }
}
