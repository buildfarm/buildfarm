// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.persistent;

import static com.google.common.base.Preconditions.checkNotNull;
import static persistent.bazel.client.PersistentWorker.TOOL_INPUT_SUBDIR;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import lombok.extern.java.Log;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkCoordinator;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerSupervisor;

/**
 * Responsible for:
 *
 * <ol>
 *   <li>Initializing a new Worker's file environment correctly
 *   <li>pre-request requirements, e.g. ensuring tool input files
 *   <li>post-response requirements, i.e. putting output files in the right place
 * </ol>
 */
@Log
public class ProtoCoordinator extends WorkCoordinator<RequestCtx, ResponseCtx, CommonsWorkerPool> {
  private static final String WORKER_INIT_LOG_SUFFIX = ".initargs.log";

  private static final ConcurrentHashMap<RequestCtx, PersistentWorker> pendingReqs =
      new ConcurrentHashMap<>();

  private static final Timer timeoutScheduler = new Timer("persistent-worker-timeout", true);

  // Synchronize writes to the tool input directory per WorkerKey
  // TODO: We only need a Set of WorkerKeys to synchronize on, but no ConcurrentHashSet
  private static final ConcurrentHashMap<WorkerKey, WorkerKey> toolInputSyncs =
      new ConcurrentHashMap<>();

  // Enforces locking on the same object given the same WorkerKey
  private static WorkerKey keyLock(WorkerKey key) {
    return toolInputSyncs.computeIfAbsent(key, k -> k);
  }

  public ProtoCoordinator(CommonsWorkerPool workerPool) {
    super(workerPool);
  }

  private ProtoCoordinator(WorkerSupervisor supervisor, int maxWorkersPerKey) {
    super(new CommonsWorkerPool(supervisor, maxWorkersPerKey));
  }

  // We copy tool inputs from the shared WorkerKey tools directory into our worker exec root,
  //    since there are multiple workers per key,
  //    and presumably there might be writes to tool inputs?
  // Tool inputs which are absolute-paths (e.g. /usr/bin/...) are not affected
  public static ProtoCoordinator ofCommonsPool(int maxWorkersPerKey) {
    WorkerSupervisor loadToolsOnCreate =
        new WorkerSupervisor() {
          @Override
          public PersistentWorker create(WorkerKey workerKey) throws Exception {
            Path keyExecRoot = workerKey.getExecRoot();
            String workerExecDir = getUniqueSubdir(keyExecRoot);
            Path workerExecRoot = keyExecRoot.resolve(workerExecDir);
            copyToolsIntoWorkerExecRoot(workerKey, workerExecRoot);

            Path initArgsLogFile = workerExecRoot.resolve(workerExecDir + WORKER_INIT_LOG_SUFFIX);
            if (!Files.exists(initArgsLogFile)) {
              StringBuilder initArgs = new StringBuilder();
              for (String s : workerKey.getCmd()) {
                initArgs.append(s).append('\n');
              }
              for (String s : workerKey.getArgs()) {
                initArgs.append(s).append('\n');
              }

              Files.write(initArgsLogFile, initArgs.toString().getBytes());
            }
            return new PersistentWorker(workerKey, workerExecDir);
          }
        };
    return new ProtoCoordinator(loadToolsOnCreate, maxWorkersPerKey);
  }

  public void copyToolInputsIntoWorkerToolRoot(WorkerKey key, WorkerInputs workerFiles)
      throws IOException {
    WorkerKey lock = keyLock(key);
    synchronized (lock) {
      try {
        // Copy tool inputs as needed
        Path workToolRoot = key.getExecRoot().resolve(PersistentWorker.TOOL_INPUT_SUBDIR);
        for (Path opToolPath : workerFiles.opToolInputs) {
          Path workToolPath = workerFiles.relativizeInput(workToolRoot, opToolPath);
          if (!Files.exists(workToolPath)) {
            workerFiles.copyInputFile(opToolPath, workToolPath);
          }
        }
      } finally {
        toolInputSyncs.remove(key);
      }
    }
  }

  private static String getUniqueSubdir(Path workRoot) {
    String uuid = UUID.randomUUID().toString();
    while (Files.exists(workRoot.resolve(uuid))) {
      uuid = UUID.randomUUID().toString();
    }
    return uuid;
  }

  // copyToolInputsIntoWorkerToolRoot() should have been called before this.
  private static void copyToolsIntoWorkerExecRoot(WorkerKey key, Path workerExecRoot)
      throws IOException {
    log.log(Level.FINE, "loadToolsIntoWorkerRoot() into: " + workerExecRoot);

    Path toolInputRoot = key.getExecRoot().resolve(TOOL_INPUT_SUBDIR);
    for (Path relPath : key.getWorkerFilesWithHashes().keySet()) {
      Path toolInputPath = toolInputRoot.resolve(relPath);
      Path execRootPath = workerExecRoot.resolve(relPath);

      FileAccessUtils.copyFile(toolInputPath, execRootPath);
    }
  }

  @Override
  public WorkRequest preWorkInit(WorkerKey key, RequestCtx request, PersistentWorker worker)
      throws IOException {
    PersistentWorker pendingWorker = pendingReqs.putIfAbsent(request, worker);
    // null means that this request was not in pendingReqs (the expected case)
    if (pendingWorker != null) {
      if (pendingWorker != worker) {
        throw new IllegalArgumentException(
            "Already have a persistent worker on the job: " + request.request);
      } else {
        throw new IllegalArgumentException(
            "Got the same request for the same worker while it's running: " + request.request);
      }
    }
    startTimeoutTimer(request);

    // Symlinking should hypothetically be faster+leaner than copying inputs, but it's buggy.
    copyNontoolInputs(request.workerInputs, worker.getExecRoot());

    return request.request;
  }

  // After the worker has finished, output files need to be visible in the operation directory
  @Override
  public ResponseCtx postWorkCleanup(
      WorkResponse response, PersistentWorker worker, RequestCtx request) throws IOException {
    pendingReqs.remove(request);

    if (response == null) {
      throw new RuntimeException("postWorkCleanup: WorkResponse was null!");
    }

    if (response.getExitCode() == 0) {
      try {
        Path workerExecRoot = worker.getExecRoot();
        moveOutputsToOperationRoot(request.filesContext, workerExecRoot);
        cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
      } catch (IOException e) {
        throw logBadCleanup(request, e);
      }
    }

    return new ResponseCtx(response, worker.flushStdErr());
  }

  private IOException logBadCleanup(RequestCtx request, IOException e) {
    WorkFilesContext context = request.filesContext;

    StringBuilder sb = new StringBuilder(122);
    sb.append("Output files failure debug for request with args<")
        .append(request.request.getArgumentsList())
        .append(">:\ngetOutputPathsList:\n")
        .append(context.outputPaths)
        .append("getOutputFilesList:\n")
        .append(context.outputFiles)
        .append("getOutputDirectoriesList:\n")
        .append(context.outputDirectories);

    log.log(Level.SEVERE, sb.toString(), e);

    return new IOException("Response was OK but failed on postWorkCleanup", e);
  }

  private void copyNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        Path execPath = workerInputs.relativizeInput(workerExecRoot, opPath);
        workerInputs.copyInputFile(opPath, execPath);
      }
    }
  }

  // Make outputs visible to the rest of Worker machinery
  // see DockerExecutor::copyOutputsOutOfContainer
  void moveOutputsToOperationRoot(WorkFilesContext context, Path workerExecRoot)
      throws IOException {
    Path opRoot = context.opRoot;

    for (String outputDir : context.outputDirectories) {
      Path outputDirPath = Path.of(outputDir);
      Files.createDirectories(outputDirPath);
    }

    for (String relOutput : context.outputFiles) {
      Path execOutputPath = workerExecRoot.resolve(relOutput);
      Path opOutputPath = opRoot.resolve(relOutput);
      // Don't fail here if the action failed to produce a file.
      // The missing file will be handled just like it is for non-worker actions.
      if (Files.exists(execOutputPath)) {
        FileAccessUtils.moveFile(execOutputPath, opOutputPath);
      }
    }
  }

  private void cleanUpNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        workerInputs.deleteInputFileIfExists(workerExecRoot, opPath);
      }
    }
  }

  /**
   * Start a timeout timer based on the request's timeout.
   *
   * @param request
   */
  private void startTimeoutTimer(RequestCtx request) {
    checkNotNull(request.timeout);
    timeoutScheduler.schedule(
        new RequestTimeoutHandler(request), Durations.toMillis(request.timeout));
  }

  private final class RequestTimeoutHandler extends TimerTask {
    private final RequestCtx request;

    private RequestTimeoutHandler(RequestCtx request) {
      this.request = request;
    }

    @Override
    public void run() {
      onTimeout(this.request, pendingReqs.get(this.request));
    }
  }

  private void onTimeout(RequestCtx request, PersistentWorker worker) {
    if (worker != null) {
      log.severe("Persistent Worker timed out on request: " + request.request);
      try {
        this.workerPool.invalidateObject(worker.getKey(), worker);
      } catch (Exception e) {
        log.severe(
            "Tried to invalidate worker for request:\n"
                + request
                + "\n\tbut got: "
                + e
                + "\n\nCalling worker.destroy() and moving on.");
        worker.destroy();
      }
    }
  }
}
