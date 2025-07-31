/**
 * Transfers data to remote storage or workers Includes input validation and error handling for robustness.
 * @param uri the uri parameter
 * @param casUpload the casUpload parameter
 * @param hash the hash parameter
 * @param data the data parameter
 * @param contentLength the contentLength parameter
 * @return the protected result
 */
/**
 * Performs specialized operation based on method logic
 * @return the uri result
 */
// Copyright 2018 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.proxy.http;

import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.net.URI;

/** Object sent through the channel pipeline to start an upload. */
final class UploadCommand {
  private final URI uri;
  private final boolean casUpload;
  private final String hash;
  private final InputStream data;
  private final long contentLength;

  protected UploadCommand(
      URI uri, boolean casUpload, String hash, InputStream data, long contentLength) {
    this.uri = Preconditions.checkNotNull(uri);
    this.casUpload = casUpload;
    this.hash = Preconditions.checkNotNull(hash);
    this.data = Preconditions.checkNotNull(data);
    this.contentLength = contentLength;
  }

  /**
   * Transfers data to remote storage or workers
   * @return the boolean result
   */
  public URI uri() {
    return uri;
  }

  /**
   * Performs specialized operation based on method logic
   * @return the string result
   */
  public boolean casUpload() {
    return casUpload;
  }

  /**
   * Performs specialized operation based on method logic
   * @return the inputstream result
   */
  public String hash() {
    return hash;
  }

  /**
   * Performs specialized operation based on method logic
   * @return the long result
   */
  public InputStream data() {
    return data;
  }

  public long contentLength() {
    return contentLength;
  }
}
