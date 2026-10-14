// Copyright 2024 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.base.Preconditions.checkNotNull;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.worker.ExecTreeWalker.Event;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.util.Map;

class ExecTree {
  private final Map<Digest, Directory> index;

  ExecTree(Map<Digest, Directory> index) {
    this.index = index;
  }

  public Path walk(
      Path start, build.buildfarm.v1test.Digest digest, FileVisitor<? super Path> visitor)
      throws IOException {
    try (ExecTreeWalker walker = new ExecTreeWalker(index, digest.getDigestFunction())) {
      Event ev = walker.walk(start, DigestUtil.toDigest(digest));
      do {
        FileVisitResult result =
            switch (ev.getType()) {
              case ENTRY -> {
                IOException ioe = ev.getIoe();
                if (ioe == null) {
                  yield visitor.visitFile(ev.getPath(), checkNotNull(ev.getAttributes()));
                } else {
                  yield visitor.visitFileFailed(ev.getPath(), ioe);
                }
              }
              case START_DIRECTORY -> {
                FileVisitResult res = visitor.preVisitDirectory(ev.getPath(), ev.getAttributes());

                // if SKIP_SIBLINGS and SKIP_SUBTREE is returned then
                // there shouldn't be any more events for the current
                // directory.
                if (res == FileVisitResult.SKIP_SUBTREE || res == FileVisitResult.SKIP_SIBLINGS) {
                  walker.pop();
                }
                yield res;
              }
              case END_DIRECTORY -> {
                FileVisitResult res = visitor.postVisitDirectory(ev.getPath(), ev.getIoe());

                // SKIP_SIBLINGS is a no-op for postVisitDirectory
                if (res == FileVisitResult.SKIP_SIBLINGS) {
                  res = FileVisitResult.CONTINUE;
                }
                yield res;
              }
              default -> throw new AssertionError("Should not get here");
            };

        if (checkNotNull(result) != FileVisitResult.CONTINUE) {
          if (result == FileVisitResult.TERMINATE) {
            break;
          }
          if (result == FileVisitResult.SKIP_SIBLINGS) {
            walker.skipRemainingSiblings();
          }
        }
        ev = walker.next();
      } while (ev != null);
    }
    return start;
  }
}
