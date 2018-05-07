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
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.longrunning.Operation;
import java.util.Set;
import java.util.function.Predicate;

public interface ShardBackplane {

  /**
   * Adds a worker's name to the set of active workers.
   */
  @ThreadSafe
  public void addWorker(String workerName);

  /**
   * Removes a worker's name from the set of active workers.
   */
  @ThreadSafe
  public void removeWorker(String workerName);

  /**
   * Returns the name of a random worker from the set of active workers.
   */
  @ThreadSafe
  public String getRandomWorker();

  /**
   * Returns a set of the names of all active workers.
   */
  @ThreadSafe
  public Set<String> getWorkerSet();

  /**
   * Checks whether or not a worker name is in the set of active workers.
   */
  @ThreadSafe
  public boolean isWorker(String workerName);

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Retrieves and returns an action result from the hash map.
   */
  @ThreadSafe
  public ActionResult getActionResult(ActionKey actionKey);

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Remove an action result from the hash map.
   */
  @ThreadSafe
  public void removeActionResult(ActionKey actionKey);

  /**
   * The AC stores full ActionResult objects in a hash map where the key is the
   * digest of the action result and the value is the actual ActionResult
   * object.
   *
   * Stores an action result in the hash map.
   */
  @ThreadSafe
  public void putActionResult(ActionKey actionKey, ActionResult actionResult);

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Adds the name of a worker to the set of workers that store a blob.
   */
  @ThreadSafe
  public void addBlobLocation(Digest blobDigest, String workerName);

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Removes the name of a worker from the set of workers that store a blob.
   */
  @ThreadSafe
  public void removeBlobLocation(Digest blobDigest, String workerName);

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Returns a random worker from the set of workers that store a blob.
   */
  @ThreadSafe
  public String getBlobLocation(Digest blobDigest);

  /**
   * The CAS is represented as a map where the key is the digest of the blob
   * that is being stored and the value is a set of the names of the workers
   * where that blob is stored.
   *
   * Returns the set of the names of all workers that store a blob.
   */
  @ThreadSafe
  public Set<String> getBlobLocationSet(Digest blobDigest);

  /**
   * Operations are stored in a hash map where the key is the name of the
   * operation and the value is the actual Operation object.
   *
   * Retrieves and returns an operation from the hash map.
   */
  public Operation getOperation(String operationName);

  /**
   * Operations are stored in a hash map where the key is the name of the
   * operation and the value is the actual Operation object.
   *
   * Stores an operation in the hash map.
   */
  public boolean putOperation(Operation operation);

  /**
   * The state of operations is tracked in a series of lists representing the
   * order in which the work is to be processed (queued, dispatched, and
   * completed).
   *
   * Moves an operation from the list of queued operations to the list of
   * dispatched operations.
   */
  public String dispatchOperation() throws InterruptedException;

  /**
   * Register a watcher for an operation
   */
  public boolean watchOperation(String operationName, Predicate<Operation> watcher);

  /**
   * Get all operations
   */
  public Iterable<String> getOperations();
}
