// Copyright 2019 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.common.InputStreamFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface ExecFileSystem extends InputStreamFactory {
  void start(Consumer<List<Digest>> onDigests, boolean skipLoad)
      throws IOException, InterruptedException;

  void stop() throws InterruptedException;

  Path root();

  ContentAddressableStorage getStorage();

  Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException;

  void destroyExecDir(Path execDir) throws IOException, InterruptedException;

  abstract class ExecBaseAttributes implements BasicFileAttributes {
    private static final FileTime EARLY = FileTime.from(0, TimeUnit.SECONDS);

    enum ExecFileType {
      FILE,
      SYMLINK,
      DIRECTORY,
    }

    private final ExecFileType type;

    ExecBaseAttributes(ExecFileType type) {
      this.type = type;
    }

    @Override
    public FileTime creationTime() {
      return EARLY;
    }

    @Override
    public boolean isDirectory() {
      return type == ExecFileType.DIRECTORY;
    }

    @Override
    public boolean isOther() {
      return false;
    }

    @Override
    public boolean isRegularFile() {
      return type == ExecFileType.FILE;
    }

    @Override
    public boolean isSymbolicLink() {
      return type == ExecFileType.SYMLINK;
    }

    @Override
    public FileTime lastAccessTime() {
      return EARLY;
    }

    @Override
    public FileTime lastModifiedTime() {
      return EARLY;
    }
  }

  class ExecDigestAttributes extends ExecBaseAttributes {
    private final Digest digest;

    ExecDigestAttributes(Digest digest, ExecFileType type) {
      super(type);
      this.digest = digest;
    }

    @Override
    public Object fileKey() {
      return digest;
    }

    @Override
    public long size() {
      return digest.getSizeBytes();
    }
  }

  class ExecFileAttributes extends ExecDigestAttributes {
    private final boolean isExecutable;

    ExecFileAttributes(Digest digest, boolean isExecutable) {
      super(digest, ExecFileType.FILE);
      this.isExecutable = isExecutable;
    }

    public boolean isExecutable() {
      return isExecutable;
    }
  }

  class ExecSymlinkAttributes extends ExecBaseAttributes {
    private final String target;

    ExecSymlinkAttributes(String target) {
      super(ExecFileType.SYMLINK);
      this.target = target;
    }

    public String target() {
      return target;
    }

    @Override
    public Object fileKey() {
      return this;
    }

    @Override
    public long size() {
      return 0;
    }
  }

  class ExecDirectoryAttributes extends ExecDigestAttributes {
    ExecDirectoryAttributes(Digest digest) {
      super(digest, ExecFileType.DIRECTORY);
    }
  }
}
