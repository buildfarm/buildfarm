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
import java.io.OutputStream;
import java.net.URI;

/** Object sent through the channel pipeline to start a download. */
final class DownloadCommand {
  private final URI uri;
  private final boolean casDownload;
  private final String hash;
  private final OutputStream out;
  private final boolean downloadContent;

  protected DownloadCommand(
      URI uri, boolean casDownload, String hash, OutputStream out, boolean downloadContent) {
    this.uri = Preconditions.checkNotNull(uri);
    this.casDownload = casDownload;
    this.hash = Preconditions.checkNotNull(hash);
    this.out = Preconditions.checkNotNull(out);
    this.downloadContent = downloadContent;
  }

  public URI uri() {
    return uri;
  }

  public boolean casDownload() {
    return casDownload;
  }

  public String hash() {
    return hash;
  }

  public OutputStream out() {
    return out;
  }

  public boolean downloadContent() {
    return downloadContent;
  }
}
