/**
 * Performs specialized operation based on method logic
 * @return the private result
 */
// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

public final class Errors {
  public static final String VIOLATION_TYPE_MISSING = "MISSING";

  public static final String VIOLATION_TYPE_INVALID = "INVALID";

  public static final String MISSING_INPUT =
      "A requested input (or the `Action` or its `Command`) was not found in the CAS.";

  private Errors() {}
}
