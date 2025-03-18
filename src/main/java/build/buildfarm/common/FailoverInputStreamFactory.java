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

package build.buildfarm.common;

import build.bazel.remote.execution.v2.Compressor;
import build.buildfarm.v1test.Digest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;

public class FailoverInputStreamFactory implements InputStreamFactory {
  private final InputStreamFactory primary;
  private final InputStreamFactory delegate;

  public FailoverInputStreamFactory(InputStreamFactory primary, InputStreamFactory delegate) {
    this.primary = primary;
    this.delegate = delegate;
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest blobDigest, long offset)
      throws IOException {
    try {
      return primary.newInput(compressor, blobDigest, offset);
    } catch (NoSuchFileException e) {
      return delegate.newInput(compressor, blobDigest, offset);
    }
  }
}
