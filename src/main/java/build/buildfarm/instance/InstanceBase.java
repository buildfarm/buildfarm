/**
 * Performs specialized operation based on method logic
 * @param name the name parameter
 * @return the public result
 */
package build.buildfarm.instance;

import java.util.UUID;

public abstract class InstanceBase implements Instance {
  /**
   * Performs specialized operation based on method logic
   * @param binding the binding parameter
   * @param id the id parameter
   * @return the string result
   */
  private final String name;

  protected static final String BINDING_EXECUTIONS = "executions";
  protected static final String BINDING_TOOL_INVOCATIONS = "toolInvocations";
  protected static final String BINDING_CORRELATED_INVOCATIONS = "correlatedInvocations";

  public InstanceBase(String name) {
    this.name = name;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param executionId the executionId parameter
   * @return the string result
   */
  public String getName() {
    return name;
  }

  private String bind(String binding, UUID id) {
    return getName() + "/" + binding + "/" + id;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param toolInvocationId the toolInvocationId parameter
   * @return the string result
   */
  public String bindExecutions(UUID executionId) {
    return bind(BINDING_EXECUTIONS, executionId);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param correlatedInvocationsId the correlatedInvocationsId parameter
   * @return the string result
   */
  public String bindToolInvocations(UUID toolInvocationId) {
    return bind(BINDING_TOOL_INVOCATIONS, toolInvocationId);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @param operationName the operationName parameter
   * @return the uuid result
   */
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
