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

package build.buildfarm.instance;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.TokenizableIterator;
import build.buildfarm.v1test.TreeIteratorToken;
import com.google.common.collect.Iterators;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.function.Function;
import javax.annotation.Nullable;

public class TreeIterator implements TokenizableIterator<TreeIterator.DirectoryEntry> {
  private final Function<Digest, ListenableFuture<Directory>> getDirectoryFuture;
  private Deque<Digest> path;
  private final ArrayDeque<Digest> parentPath;
  private final Stack<Iterator<Digest>> pointers;

  public TreeIterator(Function<Digest, ListenableFuture<Directory>> getDirectoryFuture, Digest rootDigest, String pageToken) throws IOException, InterruptedException {
    this.getDirectoryFuture = getDirectoryFuture;
    parentPath = new ArrayDeque<Digest>();
    pointers = new Stack<Iterator<Digest>>();

    Iterator<Digest> iter = Iterators.singletonIterator(rootDigest);

    Directory directory = AbstractServerInstance.getUnchecked(getDirectoryFuture.apply(rootDigest));

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
        directory = AbstractServerInstance.getUnchecked(getDirectoryFuture.apply(digest));
        if (directory == null) {
          // some directory data has disappeared, current iter
          // is correct and will be next directory fetched
          break;
        }
        iter = Iterators.transform(
            directory.getDirectoriesList().iterator(),
            directoryNode -> {
              return directoryNode.getDigest();
            });
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

  public class DirectoryEntry {
    private final Digest digest;
    @Nullable private final Directory directory;

    DirectoryEntry(Digest digest, @Nullable Directory directory) {
      this.digest = digest;
      this.directory = directory;
    }

    public Digest getDigest() {
      return digest;
    }

    @Nullable
    public Directory getDirectory() {
      return directory;
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
    try {
      Directory directory = AbstractServerInstance.getUnchecked(getDirectoryFuture.apply(digest));
      DirectoryEntry entry = new DirectoryEntry(digest, directory);
      if (directory != null) {
        /* the path to a new iter set is the path to its parent */
        parentPath.addLast(digest);
        path = parentPath.clone();
        pointers.push(Iterators.transform(
            directory.getDirectoriesList().iterator(),
            directoryNode -> directoryNode.getDigest()));
      }
      advanceIterator();
      return entry;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private TreeIteratorToken parseToken(byte[] bytes) {
    try {
      return TreeIteratorToken.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException();
    }
  }

  private MessageLite toToken() {
    return TreeIteratorToken.newBuilder()
        .addAllDirectories(path)
        .build();
  }

  public String toNextPageToken() {
    if (hasNext()) {
      return BaseEncoding.base64().encode(toToken().toByteArray());
    }
    return "";
  }
}
