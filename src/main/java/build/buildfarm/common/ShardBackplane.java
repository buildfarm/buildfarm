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

import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ThreadSafety.ThreadSafe;
import build.buildfarm.v1test.ShardDispatchedOperation;
import com.google.common.collect.ImmutableList;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public interface ShardBackplane {

  public final static class ActionCacheScanResult {
    public final String token;
    public final Iterable<Map.Entry<ActionKey, ActionResult>> entries;

    public ActionCacheScanResult(String token, Iterable<Map.Entry<ActionKey, ActionResult>> entries) {
      this.token = token;
      this.entries = entries;
    }
  }

  /**
   * Start the backplane's operation
   */
  @ThreadSafe
  public void start();

  /**
   * Stop the backplane's operation
   */
  @ThreadSafe
  public void stop();

  /**
   * Adds a worker's name to the set of active workers.
   */
  @ThreadSafe
  public void addWorker(String workerName) throws IOException;

  /**
   * Removes a worker's name from the set of active workers.
   */
  @ThreadSafe
  public void removeWorker(String workerName) throws IOException;

  /**
   * Returns the name of a random worker from the set of active workers.
   */
  @ThreadSafe
  public String getRandomWorker() throws IOException;

  /**
   * Returns a set of the names of all active workers.
   */
  @ThreadSafe
  public Set<String> getWorkerSet() throws IOException;

  /**
   * Checks whether or not a worker name is in the set of active workers.
   */
  @ThreadSafe
  public boolean isWorker(String workerName) throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Retrieves and returns an action result from the hash map.
   */
  @ThreadSafe
  public ActionResult getActionResult(ActionKey actionKey) throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Remove an action result from the hash map.
   */
  @ThreadSafe
  public void removeActionResult(ActionKey actionKey) throws IOException;

  /**
   * Bulk remove action results
   */
  @ThreadSafe
  public void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Stores an action result in the hash map.
   */
  @ThreadSafe
  public void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Adds the name of a worker to the set of workers that store a blob.
   */
  @ThreadSafe
  public void addBlobLocation(Digest blobDigest, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Adds the name of a worker to the set of workers that store multiple blobs.
   */
  @ThreadSafe
  public void addBlobsLocation(Iterable<Digest> blobDigest, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Removes the name of a worker from the set of workers that store a blob.
   */
  @ThreadSafe
  public void removeBlobLocation(Digest blobDigest, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Removes the name of a worker from the set of workers that store multiple blobs.
   */
  @ThreadSafe
  public void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Returns a random worker from the set of workers that store a blob.
   */
  @ThreadSafe
  public String getBlobLocation(Digest blobDigest) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Returns the set of the names of all workers that store a blob.
   */
  @ThreadSafe
  public Set<String> getBlobLocationSet(Digest blobDigest) throws IOException;

  @ThreadSafe
  public Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests) throws IOException;

  /**
   * Operations are stored in a hash map where the key is the name of the
   * operation and the value is the actual Operation object.
   *
   * Retrieves and returns an operation from the hash map.
   */
  @ThreadSafe
  public Operation getOperation(String operationName) throws IOException;

  /**
   * Operations are stored in a hash map where the key is the name of the
   * operation and the value is the actual Operation object.
   *
   * Stores an operation in the hash map.
   */
  @ThreadSafe
  public boolean putOperation(Operation operation, Stage stage) throws IOException;

  /**
   * The state of operations is tracked in a series of lists representing the
   * order in which the work is to be processed (queued, dispatched, and
   * completed).
   *
   * Moves an operation from the list of queued operations to the list of
   * dispatched operations.
   */
  @ThreadSafe
  public String dispatchOperation() throws InterruptedException, IOException;

  /**
   * The state of operations is tracked in a series of lists representing the
   * order in which the work is to be processed (queued, dispatched, and
   * completed).
   *
   * Updates a dispatchedOperation requeue_at and returns whether the
   * operation is still valid.
   */
  @ThreadSafe
  public boolean pollOperation(String operationName, Stage stage, long requeueAt) throws IOException;

  /**
   * Complete an operation
   */
  @ThreadSafe
  public void completeOperation(String operationName) throws IOException;

  /**
   * Delete an operation
   */
  @ThreadSafe
  public void deleteOperation(String operationName) throws IOException;

  /**
   * Register a watcher for an operation
   */
  @ThreadSafe
  public boolean watchOperation(String operationName, Predicate<Operation> watcher) throws IOException;

  /**
   * Get all dispatched operations
   */
  @ThreadSafe
  public ImmutableList<ShardDispatchedOperation> getDispatchedOperations() throws IOException;

  /**
   * Get all operations
   */
  @ThreadSafe
  public Iterable<String> getOperations() throws IOException;

  /**
   * Requeue a dispatched operation
   */
  @ThreadSafe
  public void requeueDispatchedOperation(Operation operation) throws IOException;

  /**
   * Store a directory tree and all of its descendants
   */
  @ThreadSafe
  public void putTree(Digest inputRoot, Iterable<Directory> directories) throws IOException;

  /**
   * Retrieve a directory tree and all of its descendants
   */
  @ThreadSafe
  public Iterable<Directory> getTree(Digest inputRoot) throws IOException;

  /**
   * Destroy a cached directory tree of a completed operation
   */
  @ThreadSafe
  public void removeTree(Digest inputRoot) throws IOException;

  /**
   * Get the size of the completed operations list
   */
  @ThreadSafe
  public long getCompletedOperationsCount() throws IOException;

  /**
   * Pop the oldest completed operation
   */
  @ThreadSafe
  public String popOldestCompletedOperation() throws IOException;

  /**
   * Page through action cache
   */
  @ThreadSafe
  public ActionCacheScanResult scanActionCache(String scanToken, int count) throws IOException;
}
