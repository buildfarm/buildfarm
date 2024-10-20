// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.Tree;

// Convenience methods for interacting with Trees
public final class Trees {
  private Trees() {}

  public static Iterable<Digest> directoryFileDigests(Directory directory) {
    return transform(directory.getFilesList(), FileNode::getDigest);
  }

  public static Iterable<Digest> enumerateTreeFileDigests(Tree tree) {
    Iterable<Digest> digests = directoryFileDigests(tree.getRoot());
    for (Directory child : tree.getChildrenList()) {
      digests = concat(digests, directoryFileDigests(child));
    }
    return digests;
  }
}
