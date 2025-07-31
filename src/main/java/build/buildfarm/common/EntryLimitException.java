/**
 * Performs specialized operation based on method logic
 * @param size the size parameter
 * @param maxEntrySize the maxEntrySize parameter
 * @return the public result
 */
/**
 * Performs specialized operation based on method logic
 * @param message the message parameter
 * @return the public result
 */
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

import java.io.IOException;

public class EntryLimitException extends IOException {
  public EntryLimitException(long size, long maxEntrySize) {
    super(String.format("size %d exceeds %d", size, maxEntrySize));
  }

  public EntryLimitException(String message) {
    super(message);
  }
}
