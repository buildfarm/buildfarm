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

package build.buildfarm.backplane;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.CasIndexResults;
import build.buildfarm.common.DigestUtil.ActionKey;
import build.buildfarm.common.ThreadSafety.ThreadSafe;
import build.buildfarm.common.Watcher;
import build.buildfarm.common.function.InterruptingRunnable;
import build.buildfarm.instance.Instance;
import build.buildfarm.operations.FindOperationsResults;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.GetClientStartTimeRequest;
import build.buildfarm.v1test.GetClientStartTimeResult;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.longrunning.Operation;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Backplane {
  final class ActionCacheScanResult {
    public final String token;
    public final Iterable<Map.Entry<ActionKey, ActionResult>> entries;

    public ActionCacheScanResult(
        String token, Iterable<Map.Entry<ActionKey, ActionResult>> entries) {
      this.token = token;
      this.entries = entries;
    }
  }

  /**
   * Register a runnable for when the backplane cannot guarantee watch deliveries. This runnable may
   * throw InterruptedException
   *
   * <p>onSubscribe is called via the subscription thread, and is not called when operations are not
   * listened to
   */
  void setOnUnsubscribe(InterruptingRunnable onUnsubscribe);

  /** Start the backplane's operation */
  @ThreadSafe
  void start(String publicClientName) throws IOException;

  /** Stop the backplane's operation */
  @ThreadSafe
  void stop() throws InterruptedException;

  /** Indicates whether the backplane has been stopped */
  @ThreadSafe
  boolean isStopped();

  /** Adds a worker to the set of active workers. */
  @ThreadSafe
  void addWorker(ShardWorker shardWorker) throws IOException;

  /**
   * Removes a worker's name from the set of active workers.
   *
   * <p>Return true if the worker was removed, and false if it was not a member of the set.
   */
  @ThreadSafe
  boolean removeWorker(String workerName, String reason) throws IOException;

  @ThreadSafe
  CasIndexResults reindexCas(String workerName) throws IOException;

  @ThreadSafe
  void deregisterWorker(String hostName) throws IOException;

  @ThreadSafe
  FindOperationsResults findOperations(Instance instance, String filterPredicate)
      throws IOException;

  /** Returns a set of the names of all active workers. */
  @ThreadSafe
  Set<String> getWorkers() throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Retrieves and returns an action result from the hash map.
   */
  @ThreadSafe
  ActionResult getActionResult(ActionKey actionKey) throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Remove an action result from the hash map.
   */
  @ThreadSafe
  void removeActionResult(ActionKey actionKey) throws IOException;

  /** Bulk remove action results */
  @ThreadSafe
  void removeActionResults(Iterable<ActionKey> actionKeys) throws IOException;

  /**
   * Identify an action that should not be executed, and respond to all requests it matches with
   * failover-compatible responses.
   */
  @ThreadSafe
  void blacklistAction(String actionId) throws IOException;

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the digest of the action
   * result and the value is the actual ActionResult object.
   *
   * <p>Stores an action result in the hash map.
   */
  @ThreadSafe
  void putActionResult(ActionKey actionKey, ActionResult actionResult) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Adds the name of a worker to the set of workers that store a blob.
   */
  @ThreadSafe
  void addBlobLocation(Digest blobDigest, String workerName) throws IOException;

  /** Remove or add workers to a blob's location set as requested */
  @ThreadSafe
  void adjustBlobLocations(Digest blobDigest, Set<String> addWorkers, Set<String> removeWorkers)
      throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Adds the name of a worker to the set of workers that store multiple blobs.
   */
  @ThreadSafe
  void addBlobsLocation(Iterable<Digest> blobDigest, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Removes the name of a worker from the set of workers that store a blob.
   */
  @ThreadSafe
  void removeBlobLocation(Digest blobDigest, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Removes the name of a worker from the set of workers that store multiple blobs.
   */
  @ThreadSafe
  void removeBlobsLocation(Iterable<Digest> blobDigests, String workerName) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Returns a random worker from the set of workers that store a blob.
   */
  @ThreadSafe
  String getBlobLocation(Digest blobDigest) throws IOException;

  /**
   * The CAS is represented as a map where the key is the digest of the blob that is being stored
   * and the value is a set of the names of the workers where that blob is stored.
   *
   * <p>Returns the set of the names of all workers that store a blob.
   */
  @ThreadSafe
  Set<String> getBlobLocationSet(Digest blobDigest) throws IOException;

  @ThreadSafe
  Map<Digest, Set<String>> getBlobDigestsWorkers(Iterable<Digest> blobDigests) throws IOException;

  /**
   * Operations are stored in a hash map where the key is the name of the operation and the value is
   * the actual Operation object.
   *
   * <p>Retrieves and returns an operation from the hash map.
   */
  @ThreadSafe
  Operation getOperation(String operationName) throws IOException;

  /**
   * Operations are stored in a hash map where the key is the name of the operation and the value is
   * the actual Operation object.
   *
   * <p>Stores an operation in the hash map.
   */
  @ThreadSafe
  boolean putOperation(Operation operation, ExecutionStage.Value stage) throws IOException;

  @ThreadSafe
  ExecuteEntry deprequeueOperation() throws IOException, InterruptedException;

  /**
   * The state of operations is tracked in a series of lists representing the order in which the
   * work is to be processed (queued, dispatched, and completed).
   *
   * <p>Moves an operation from the list of queued operations to the list of dispatched operations.
   */
  @ThreadSafe
  QueueEntry dispatchOperation(List<Platform.Property> provisions)
      throws IOException, InterruptedException;

  /**
   * Pushes an operation onto the head of the list of queued operations after a rejection which does
   * not require revalidation
   */
  @ThreadSafe
  void rejectOperation(QueueEntry queueEntry) throws IOException;

  /**
   * Updates the backplane to indicate that the operation is being queued and should not be
   * considered immediately lost
   */
  @ThreadSafe
  void queueing(String operationName) throws IOException;

  /**
   * The state of operations is tracked in a series of lists representing the order in which the
   * work is to be processed (queued, dispatched, and completed).
   *
   * <p>Updates a dispatchedOperation requeue_at and returns whether the operation is still valid.
   */
  @ThreadSafe
  boolean pollOperation(QueueEntry queueEntry, ExecutionStage.Value stage, long requeueAt)
      throws IOException;

  /** Complete an operation */
  @ThreadSafe
  void completeOperation(String operationName) throws IOException;

  /** Delete an operation */
  @ThreadSafe
  void deleteOperation(String operationName) throws IOException;

  /** Register a watcher for an operation */
  @ThreadSafe
  ListenableFuture<Void> watchOperation(String operationName, Watcher watcher);

  /** Get all dispatched operations */
  @ThreadSafe
  ImmutableList<DispatchedOperation> getDispatchedOperations() throws IOException;

  /** Get all operations */
  @ThreadSafe
  Iterable<String> getOperations();

  /** Requeue a dispatched operation */
  @ThreadSafe
  void requeueDispatchedOperation(QueueEntry queueEntry) throws IOException;

  @ThreadSafe
  void prequeue(ExecuteEntry executeEntry, Operation operation) throws IOException;

  @ThreadSafe
  void queue(QueueEntry queueEntry, Operation operation) throws IOException;

  /** Page through action cache */
  @ThreadSafe
  ActionCacheScanResult scanActionCache(String scanToken, int count) throws IOException;

  /** Test for whether a request is blacklisted */
  @ThreadSafe
  boolean isBlacklisted(RequestMetadata requestMetadata) throws IOException;

  /** Test for whether an operation may be queued */
  @ThreadSafe
  boolean canQueue() throws IOException;

  /** Test for whether an operation may be prequeued */
  @ThreadSafe
  boolean canPrequeue() throws IOException;

  @ThreadSafe
  BackplaneStatus backplaneStatus() throws IOException;

  @ThreadSafe
  Boolean propertiesEligibleForQueue(List<Platform.Property> provisions);

  @ThreadSafe
  GetClientStartTimeResult getClientStartTime(GetClientStartTimeRequest request) throws IOException;
}
