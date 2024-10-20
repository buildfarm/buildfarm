// Copyright 2020 The Buildfarm Authors. All rights reserved.
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

/**
 * @class StringVisitor
 * @brief A string visitor.
 * @details Used to visit strings in a generic context.
 */
public abstract class StringVisitor {
  /**
   * @brief The visit interface to be implemented.
   * @details Inherited classes but implement visit.
   * @param str The visited string.
   */
  public abstract void visit(String str);
}
