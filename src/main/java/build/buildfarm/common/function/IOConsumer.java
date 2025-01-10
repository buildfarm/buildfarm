// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common.function;

import java.io.IOException;

/**
 * Represents an operation that accepts a single input argument and returns no result, and may throw
 * an IOException, implying that calls may perform I/O. Unlike most other functional interfaces,
 * {@code IOConsumer} is expected to operate via side-effects.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #accept(Object)}.
 *
 * @param <T> the type of the input to the operation
 */
@FunctionalInterface
public interface IOConsumer<T> {
  /**
   * Performs this I/O operation on the given argument.
   *
   * @param t the input argument
   */
  void accept(T t) throws IOException;
}
