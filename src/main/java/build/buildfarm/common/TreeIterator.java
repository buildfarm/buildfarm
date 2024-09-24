// Copyright 2017 The Bazel Authors. All rights reserved.
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

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.buildfarm.v1test.TreeIteratorToken;
import com.google.common.collect.Iterators;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import javax.annotation.Nullable;
import lombok.Getter;

public class TreeIterator implements TokenizableIterator<TreeIterator.DirectoryEntry> {
  private final DirectoryFetcher directoryFetcher;
  private final DigestFunction.Value digestFunction;
  private Deque<Digest> path;
  private final ArrayDeque<Digest> parentPath;
  private final Stack<Iterator<Digest>> pointers;

  @FunctionalInterface
  public interface DirectoryFetcher {
    Directory fetch(build.buildfarm.v1test.Digest digest);
  }

  public TreeIterator(
      DirectoryFetcher directoryFetcher,
      build.buildfarm.v1test.Digest rootDigest,
      String pageToken) {
    this.directoryFetcher = directoryFetcher;
    digestFunction = rootDigest.getDigestFunction();
    parentPath = new ArrayDeque<>();
    pointers = new Stack<>();

    Iterator<Digest> iter = Iterators.singletonIterator(DigestUtil.toDigest(rootDigest));

    // initial page token is empty
    if (!pageToken.isEmpty()) {
      TreeIteratorToken token = parseToken(BaseEncoding.base64().decode(pageToken));

      for (Digest digest : token.getDirectoriesList()) {
        boolean found = false;
        while (!found && iter.hasNext()) {
          if (iter.next().equals(digest)) {
            found = true;
          }
        }
        if (!found) {
          throw new IllegalArgumentException();
        }
        parentPath.addLast(digest);
        pointers.push(iter);
        Directory directory = getDirectory(digest);
        if (directory == null) {
          // some directory data has disappeared, current iter
          // is correct and will be next directory fetched
          break;
        }
        iter =
            Iterators.transform(
                directory.getDirectoriesList().iterator(), DirectoryNode::getDigest);
      }
    }
    pointers.push(iter);
    path = parentPath.clone();
    advanceIterator();
  }

  @Override
  public boolean hasNext() {
    return !pointers.isEmpty() && pointers.peek().hasNext();
  }

  private void advanceIterator() {
    while (!pointers.isEmpty()) {
      Iterator<Digest> iter = pointers.pop();
      if (iter.hasNext()) {
        pointers.push(iter);
        return;
      }
      if (!parentPath.isEmpty()) {
        parentPath.removeLast();
      }
    }
  }

  @Getter
  public static class DirectoryEntry {
    private final Digest digest;
    @Nullable private final Directory directory;

    public DirectoryEntry(Digest digest, @Nullable Directory directory) {
      this.digest = digest;
      this.directory = directory;
    }
  }

  @Override
  public DirectoryEntry next() throws NoSuchElementException {
    Iterator<Digest> iter = pointers.peek();
    if (!iter.hasNext()) {
      throw new NoSuchElementException();
    }
    /* we can have null directories in our list
     * if members of the tree have been removed from
     * the cas.  we return this to retain the information
     * (and simplify the interface) that they have been
     * removed. */
    Digest digest = iter.next();
    Directory directory = getDirectory(digest);
    if (directory != null) {
      /* the path to a new iter set is the path to its parent */
      parentPath.addLast(digest);
      path = parentPath.clone();
      pointers.push(
          Iterators.transform(directory.getDirectoriesList().iterator(), DirectoryNode::getDigest));
    }
    advanceIterator();
    return new DirectoryEntry(digest, directory);
  }

  private @Nullable Directory getDirectory(Digest digest) {
    if (digest.getSizeBytes() == 0) {
      return Directory.getDefaultInstance();
    }
    return directoryFetcher.fetch(DigestUtil.fromDigest(digest, digestFunction));
  }

  private TreeIteratorToken parseToken(byte[] bytes) {
    try {
      return TreeIteratorToken.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException();
    }
  }

  private MessageLite toToken() {
    return TreeIteratorToken.newBuilder().addAllDirectories(path).build();
  }

  public String toNextPageToken() {
    if (hasNext()) {
      return BaseEncoding.base64().encode(toToken().toByteArray());
    }
    return "";
  }
}
