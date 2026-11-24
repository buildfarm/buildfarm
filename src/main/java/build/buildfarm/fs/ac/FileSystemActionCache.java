// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.fs.ac;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.ac.ActionCache;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.io.FileStoreMover;
import build.buildfarm.v1test.Digest;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Deadline;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class FileSystemActionCache implements ActionCache {
  private final Path path;
  private final TextDB<Map.Entry<ActionKey, Digest>> db;
  private final ContentAddressableStorage storage;
  private final ActionCache delegate;
  private final Executor executor;
  private final DigestUtil digestUtil;
  private final LoadingCache<ActionKey, Digest> cache;
  private final int saveIntervalSecs;
  private volatile Deadline saveAfter;

  private static Map.Entry<ActionKey, Digest> parser(String line) {
    String[] fields = line.split(",");
    return new AbstractMap.SimpleEntry<>(
        DigestUtil.asActionKey(DigestUtil.parseDigest(fields[0])),
        DigestUtil.parseDigest(fields[1]));
  }

  private static String printer(Map.Entry<ActionKey, Digest> entry) {
    return format(
        "%s,%s\n",
        DigestUtil.toString(entry.getKey().getDigest()), DigestUtil.toString(entry.getValue()));
  }

  public FileSystemActionCache(
      int maxSize,
      Path path,
      ContentAddressableStorage storage,
      DigestUtil digestUtil,
      int saveIntervalSecs,
      ActionCache delegate,
      Executor executor)
      throws IOException {
    this.path = path;
    this.storage = storage;
    this.digestUtil = digestUtil;
    this.saveIntervalSecs = saveIntervalSecs;
    this.delegate = delegate;
    this.executor = executor;
    cache = Caffeine.newBuilder().maximumSize(maxSize).build(key -> null);
    saveAfter = Deadline.after(saveIntervalSecs, SECONDS);
    FileStoreMover fileStoreMover = FileStoreMover.probe(path.getParent());
    Function<String, Map.Entry<ActionKey, Digest>> parser = FileSystemActionCache::parser;
    Function<Map.Entry<ActionKey, Digest>, String> printer = FileSystemActionCache::printer;
    db = new TextDB(fileStoreMover, parser, printer);
    try (BufferedReader br = Files.newBufferedReader(path)) {
      for (Map.Entry<ActionKey, Digest> entry : db.entries(br)) {
        cache.put(entry.getKey(), entry.getValue());
      }
    } catch (NoSuchFileException e) {
      // ignore
    }
  }

  private void save() {
    try {
      db.save(cache.asMap().entrySet().iterator(), path);
    } catch (Exception e) {
      log.log(Level.SEVERE, "error saving ac state", e);
    }
  }

  /** Not reentrant, not thread safe */
  private void maybeSave() {
    if (saveAfter == null || !saveAfter.isExpired()) {
      return;
    }
    saveAfter = null;
    executor.execute(
        () -> {
          try {
            save();
          } finally {
            saveAfter = Deadline.after(saveIntervalSecs, SECONDS);
          }
        });
  }

  private ListenableFuture<ActionResult> readThroughTransform(ActionKey actionKey) {
    return transform(
        delegate.get(actionKey),
        result -> {
          readThrough(actionKey, result);
          return result;
        },
        executor);
  }

  @Override
  public ListenableFuture<ActionResult> get(ActionKey actionKey) {
    Digest digest = cache.get(actionKey);
    if (digest == null) {
      // should probably aggregate these while in flight...
      return readThroughTransform(actionKey);
    }
    try {
      // TODO: pick from async types
      Blob blob = storage.get(digest);
      maybeSave();
      if (blob == null) {
        // yeah, do this immediately?
        cache.invalidate(actionKey);
        return readThroughTransform(actionKey);
      }
      // should we tell the delegate about this access?
      return immediateFuture(ActionResult.parseFrom(blob.getData()));
    } catch (InvalidProtocolBufferException e) {
      log.log(
          Level.SEVERE,
          format("Invalid ActionResult %s", DigestUtil.toString(actionKey.getDigest())),
          e);
      return immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<ActionResult> put(ActionKey actionKey, ActionResult actionResult)
      throws IOException, InterruptedException {
    readThrough(actionKey, actionResult);
    return transform(
        delegate.put(actionKey, actionResult), delegateResult -> actionResult, executor);
  }

  @Override
  public void invalidate(ActionKey actionKey) {
    cache.invalidate(actionKey);
    delegate.invalidate(actionKey);
  }

  @Override
  public void readThrough(ActionKey actionKey, ActionResult actionResult) {
    try {
      Blob blob = new Blob(actionResult.toByteString(), digestUtil);
      storage.put(blob);
      cache.put(actionKey, blob.getDigest());
      maybeSave();
    } catch (InterruptedException e) {
      // ignore?
    }
  }

  public void stop() {
    save();
    delegate.stop();
  }
}
