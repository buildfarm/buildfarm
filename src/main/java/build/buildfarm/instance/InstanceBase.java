// Copyright 2024-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance;

import java.util.UUID;

public abstract class InstanceBase implements Instance {
  private final String name;

  protected static final String BINDING_EXECUTIONS = "executions";
  protected static final String BINDING_TOOL_INVOCATIONS = "toolInvocations";
  protected static final String BINDING_CORRELATED_INVOCATIONS = "correlatedInvocations";

  public InstanceBase(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  private String bind(String binding, UUID id) {
    return getName() + "/" + binding + "/" + id;
  }

  @Override
  public String bindExecutions(UUID executionId) {
    return bind(BINDING_EXECUTIONS, executionId);
  }

  @Override
  public String bindToolInvocations(UUID toolInvocationId) {
    return bind(BINDING_TOOL_INVOCATIONS, toolInvocationId);
  }

  @Override
  public String bindCorrelatedInvocations(UUID correlatedInvocationsId) {
    return bind(BINDING_CORRELATED_INVOCATIONS, correlatedInvocationsId);
  }

  @Override
  public UUID unbindExecutions(String operationName) {
    String instanceBinding = getName() + "/";
    if (operationName.startsWith(instanceBinding)) {
      operationName = operationName.substring(instanceBinding.length());
    }
    if (!operationName.startsWith(BINDING_EXECUTIONS)) {
      // throw format?
      return null;
    }
    int bindingExecutionsLength = BINDING_EXECUTIONS.length();
    operationName = operationName.substring(bindingExecutionsLength);
    if (operationName.charAt(0) != '/') {
      // throw format?
      return null;
    }
    return UUID.fromString(operationName.substring(1));
  }
}
