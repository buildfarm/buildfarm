/**
 * Retrieves a blob from the Content Addressable Storage
 * @param path the path parameter
 * @param exceptions the exceptions parameter
 * @return the string result
 */
/**
 * Stores a blob in the Content Addressable Storage
 * @param path the path parameter
 * @param digest the digest parameter
 * @param exceptions the exceptions parameter
 * @return the public result
 */
// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import build.buildfarm.v1test.Digest;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import lombok.Getter;

@Getter
public class PutDirectoryException extends IOException {
  private final Path path;
  private final Digest digest;
  private final List<Throwable> exceptions;

  private static String getErrorMessage(Path path, List<Throwable> exceptions) {
    return String.format("%s: %d %s: %s", path, exceptions.size(), "exceptions", exceptions);
  }

  public PutDirectoryException(Path path, Digest digest, List<Throwable> exceptions) {
    // When printing the exception, show the captured sub-exceptions.
    super(getErrorMessage(path, exceptions));
    this.path = path;
    this.digest = digest;
    this.exceptions = exceptions;
    for (Throwable exception : exceptions) {
      addSuppressed(exception);
    }
  }
}
