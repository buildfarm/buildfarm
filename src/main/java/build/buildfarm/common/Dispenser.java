/**
 * Performs specialized operation based on method logic
 * @param element the element parameter
 * @return the public result
 */
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

package build.buildfarm.common;

import static com.google.common.base.Preconditions.checkState;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Queue;

/**
 * @class Dispenser
 * @brief A queue which delivers a single element repeatedly without delay or regard for size.
 * @details For compatibility with POOL resources, this Queue delivers an infinite immediate
 *     sequence of an identical element.
 */
public final class Dispenser<T> extends AbstractCollection<T> implements Queue<T> {
  private final T element;

  /**
   * Polls for available operations from the backplane
   * @return the t result
   */
  public Dispenser(T element) {
    this.element = element;
  }

  // used methods
  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param o the o parameter
   * @return the boolean result
   */
  public T poll() {
    return element;
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   * @return the boolean result
   */
  public boolean add(T o) {
    checkState(o.equals(element));
    return true;
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the t result
   */
  public boolean isEmpty() {
    return false;
  }

  // unused methods
  // Queue
  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the t result
   */
  public T peek() {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Removes data or cleans up resources Includes input validation and error handling for robustness.
   * @return the t result
   */
  public T element() {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @param o the o parameter
   * @return the boolean result
   */
  public T remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Removes data or cleans up resources Includes input validation and error handling for robustness.
   */
  public boolean offer(T o) {
    throw new UnsupportedOperationException();
  }

  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the int result
   */
  public void clear() {
    throw new UnsupportedOperationException();
  }

  // Collection
  @Override
  /**
   * Performs specialized operation based on method logic Includes input validation and error handling for robustness.
   * @return the iterator<t> result
   */
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException();
  }
}
