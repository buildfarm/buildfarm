// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class OpenTelemetryProviderTest {
  @Test
  public void usesGlobalOpenTelemetryWhenAlreadyInitialized() {
    OpenTelemetry globalOpenTelemetry = OpenTelemetrySdk.builder().build();
    AtomicBoolean autoConfigurationCalled = new AtomicBoolean();

    OpenTelemetry openTelemetry =
        OpenTelemetryProvider.get(
            true,
            () -> globalOpenTelemetry,
            () -> {
              autoConfigurationCalled.set(true);
              return OpenTelemetry.noop();
            });

    assertThat(openTelemetry).isSameInstanceAs(globalOpenTelemetry);
    assertThat(autoConfigurationCalled.get()).isFalse();
  }

  @Test
  public void autoConfiguresWhenGlobalOpenTelemetryIsNotInitialized() {
    OpenTelemetry autoConfiguredOpenTelemetry = OpenTelemetrySdk.builder().build();
    AtomicBoolean globalOpenTelemetryAccessed = new AtomicBoolean();

    OpenTelemetry openTelemetry =
        OpenTelemetryProvider.get(
            false,
            () -> {
              globalOpenTelemetryAccessed.set(true);
              return OpenTelemetry.noop();
            },
            () -> autoConfiguredOpenTelemetry);

    assertThat(openTelemetry).isSameInstanceAs(autoConfiguredOpenTelemetry);
    assertThat(globalOpenTelemetryAccessed.get()).isFalse();
  }
}
