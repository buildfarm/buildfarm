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
    if (operationName.charAt(bindingExecutionsLength) != '/') {
      // throw format?
      return null;
    }
    return UUID.fromString(operationName.substring(1));
  }
}
