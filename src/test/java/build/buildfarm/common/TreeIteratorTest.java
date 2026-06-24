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

package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.buildfarm.common.TreeIterator.DirectoryEntry;
import build.buildfarm.v1test.Digest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TreeIteratorTest {
  private static final build.bazel.remote.execution.v2.DigestFunction.Value DIGEST_FUNCTION =
      build.bazel.remote.execution.v2.DigestFunction.Value.SHA256;

  // In-memory directory store keyed by digest hash. Stands in for a CAS-backed fetcher.
  private final Map<String, Directory> directoriesByHash = new HashMap<>();

  private final TreeIterator.DirectoryFetcher fetcher =
      digest -> directoriesByHash.get(digest.getHash());

  // A REAPI digest, as embedded in DirectoryNode children. Non-zero size forces a fetch.
  private static build.bazel.remote.execution.v2.Digest reapiDigest(String hash, long size) {
    return build.bazel.remote.execution.v2.Digest.newBuilder()
        .setHash(hash)
        .setSizeBytes(size)
        .build();
  }

  // The root digest is supplied as a v1test digest carrying the digest function.
  private static Digest rootDigest(String hash, long size) {
    return DigestUtil.buildDigest(hash, size, DIGEST_FUNCTION);
  }

  // Registers a directory under a hash and returns a child node referencing it.
  private DirectoryNode register(String name, String hash, Directory directory) {
    directoriesByHash.put(hash, directory);
    return DirectoryNode.newBuilder()
        .setName(name)
        .setDigest(reapiDigest(hash, /* size= */ 2))
        .build();
  }

  private static List<DirectoryEntry> drain(TreeIterator iterator) {
    List<DirectoryEntry> entries = new ArrayList<>();
    while (iterator.hasNext()) {
      entries.add(iterator.next());
    }
    return entries;
  }

  @Test
  public void singleEmptyRootYieldsOneEntry() {
    directoriesByHash.put("root", Directory.getDefaultInstance());
    TreeIterator iterator = new TreeIterator(fetcher, rootDigest("root", 2), "");

    List<DirectoryEntry> entries = drain(iterator);

    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).getDigest().getHash()).isEqualTo("root");
    assertThat(entries.get(0).getDirectory()).isEqualTo(Directory.getDefaultInstance());
  }

  @Test
  public void linearChainIsTraversedRootToLeaf() {
    Directory leaf = Directory.getDefaultInstance();
    Directory mid = Directory.newBuilder().addDirectories(register("leaf", "leaf", leaf)).build();
    Directory root = Directory.newBuilder().addDirectories(register("mid", "mid", mid)).build();
    directoriesByHash.put("root", root);

    List<DirectoryEntry> entries = drain(new TreeIterator(fetcher, rootDigest("root", 2), ""));

    List<String> hashes = new ArrayList<>();
    for (DirectoryEntry entry : entries) {
      hashes.add(entry.getDigest().getHash());
    }
    assertThat(hashes).containsExactly("root", "mid", "leaf").inOrder();
  }

  @Test
  public void branchingTreeIsTraversedDepthFirst() {
    Directory c = Directory.getDefaultInstance();
    Directory a = Directory.newBuilder().addDirectories(register("c", "c", c)).build();
    Directory b = Directory.getDefaultInstance();
    Directory root =
        Directory.newBuilder()
            .addDirectories(register("a", "a", a))
            .addDirectories(register("b", "b", b))
            .build();
    directoriesByHash.put("root", root);

    List<DirectoryEntry> entries = drain(new TreeIterator(fetcher, rootDigest("root", 2), ""));

    List<String> hashes = new ArrayList<>();
    for (DirectoryEntry entry : entries) {
      hashes.add(entry.getDigest().getHash());
    }
    // depth-first: descend through a -> c before visiting sibling b
    assertThat(hashes).containsExactly("root", "a", "c", "b").inOrder();
  }

  @Test
  public void zeroSizeChildResolvesToEmptyDirectoryWithoutFetch() {
    // A zero-size digest is resolved to the default (empty) Directory without consulting the
    // fetcher, so no entry is registered for "empty".
    DirectoryNode emptyChild =
        DirectoryNode.newBuilder()
            .setName("empty")
            .setDigest(reapiDigest("empty", /* size= */ 0))
            .build();
    Directory root = Directory.newBuilder().addDirectories(emptyChild).build();
    directoriesByHash.put("root", root);

    List<DirectoryEntry> entries = drain(new TreeIterator(fetcher, rootDigest("root", 2), ""));

    assertThat(entries).hasSize(2);
    assertThat(entries.get(1).getDigest().getHash()).isEqualTo("empty");
    assertThat(entries.get(1).getDirectory()).isEqualTo(Directory.getDefaultInstance());
  }

  @Test
  public void missingDirectoryYieldsEntryWithNullDirectory() {
    // "gone" has a non-zero size but is absent from the store: the fetcher returns null, modeling
    // a directory removed from the CAS.
    DirectoryNode goneChild =
        DirectoryNode.newBuilder()
            .setName("gone")
            .setDigest(reapiDigest("gone", /* size= */ 2))
            .build();
    Directory root = Directory.newBuilder().addDirectories(goneChild).build();
    directoriesByHash.put("root", root);

    List<DirectoryEntry> entries = drain(new TreeIterator(fetcher, rootDigest("root", 2), ""));

    assertThat(entries).hasSize(2);
    assertThat(entries.get(1).getDigest().getHash()).isEqualTo("gone");
    assertThat(entries.get(1).getDirectory()).isNull();
  }

  @Test
  public void pageTokenResumesTraversalWhereItLeftOff() {
    Directory c = Directory.getDefaultInstance();
    Directory a = Directory.newBuilder().addDirectories(register("c", "c", c)).build();
    Directory b = Directory.getDefaultInstance();
    Directory root =
        Directory.newBuilder()
            .addDirectories(register("a", "a", a))
            .addDirectories(register("b", "b", b))
            .build();
    directoriesByHash.put("root", root);

    List<String> full = new ArrayList<>();
    for (DirectoryEntry entry : drain(new TreeIterator(fetcher, rootDigest("root", 2), ""))) {
      full.add(entry.getDigest().getHash());
    }

    // Consume a single entry, capture the resumption token, then resume in a fresh iterator.
    TreeIterator first = new TreeIterator(fetcher, rootDigest("root", 2), "");
    List<String> resumed = new ArrayList<>();
    resumed.add(first.next().getDigest().getHash());
    String token = first.toNextPageToken();
    assertThat(token).isNotEmpty();

    TreeIterator second = new TreeIterator(fetcher, rootDigest("root", 2), token);
    for (DirectoryEntry entry : drain(second)) {
      resumed.add(entry.getDigest().getHash());
    }

    assertThat(resumed).isEqualTo(full);
  }

  @Test
  public void exhaustedIteratorProducesEmptyPageToken() {
    directoriesByHash.put("root", Directory.getDefaultInstance());
    TreeIterator iterator = new TreeIterator(fetcher, rootDigest("root", 2), "");

    drain(iterator);

    assertThat(iterator.toNextPageToken()).isEmpty();
  }

  @Test
  public void malformedPageTokenThrows() {
    directoriesByHash.put("root", Directory.getDefaultInstance());

    assertThrows(
        IllegalArgumentException.class,
        () -> new TreeIterator(fetcher, rootDigest("root", 2), "!!!not-base64!!!"));
  }
}
