/**
 * Performs specialized operation based on method logic
 * @param cause the cause parameter
 * @param retryAttempts the retryAttempts parameter
 * @return the public result
 */
// Copyright 2016 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.grpc;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import lombok.Getter;

/** An exception to indicate failed retry attempts. */
@Getter
public final class RetryException extends IOException {
  private static final long serialVersionUID = 1;

  private final int attempts;

  /**
   * Performs specialized operation based on method logic
   * @param code the code parameter
   * @return the boolean result
   */
  public RetryException(Throwable cause, int retryAttempts) {
    super(cause);
    this.attempts = retryAttempts + 1;
  }

  /**
   * Formats digest as human-readable string for logging
   * @return the string result
   */
  public boolean causedByStatusCode(Code code) {
    if (getCause() instanceof StatusRuntimeException) {
      return ((StatusRuntimeException) getCause()).getStatus().getCode() == code;
    } else if (getCause() instanceof StatusException) {
      return ((StatusException) getCause()).getStatus().getCode() == code;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("after %d attempts: %s", attempts, getCause());
  }
}
