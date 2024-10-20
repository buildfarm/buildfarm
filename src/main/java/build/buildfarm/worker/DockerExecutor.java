// Copyright 2021 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.ActionResult;
import build.buildfarm.common.MapUtils;
import build.buildfarm.common.io.Utils;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CopyArchiveFromContainerCmd;
import com.github.dockerjava.api.command.CopyArchiveToContainerCmd;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecStartCmd;
import com.github.dockerjava.api.command.InspectExecCmd;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.rpc.Code;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * @class DockerExecutor
 * @brief Execute an action inside a specified container.
 * @details There are different ways to execute an action. Historically, we've executed the action
 *     on the same host machine as the worker. Doing this can limit the flexibility for actions that
 *     require different operating systems or system dependencies. The inability to execute actions
 *     under custom docker containers, creates the following issues. The deployment of different
 *     workers (due to different worker images). Users that need to build in a container often
 *     forfeit using remote execution / caching. Users are unable to test system changes without
 *     re-deploying buildfarm workers. To resolve these issues, actions can specify their own
 *     containers which buildfarm will fetch and use. This is known as the "bring your own
 *     container" model for action execution. Bazel toolchains expects you to provide container
 *     images when defining platform configurations in bazel.
 */
@Log
public class DockerExecutor {
  /**
   * @brief Run the action using the docker client and populate the results.
   * @details This will fetch any images as needed, spawn a container for execution, and clean up
   *     docker resources if requested.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @param resultBuilder The action results to populate.
   * @return Grpc code as to whether buildfarm was able to run the action.
   * @note Suggested return identifier: code.
   */
  public static Code runActionWithDocker(
      DockerClient dockerClient,
      DockerExecutorSettings settings,
      ActionResult.Builder resultBuilder)
      throws InterruptedException, IOException {
    String containerId = prepareRequestedContainer(dockerClient, settings);
    String execId = runActionInsideContainer(dockerClient, settings, containerId, resultBuilder);
    extractInformationFromContainer(dockerClient, settings, containerId, execId, resultBuilder);
    cleanUpContainer(dockerClient, containerId);
    return Code.OK;
  }

  /**
   * @brief Setup the container for the action.
   * @details This ensures the image is fetched, the container is started, and that the container
   *     has proper visibility to the action's execution root. After this call it should be safe to
   *     spawn an action inside the container.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @return The ID of the started container.
   * @note Suggested return identifier: containerId.
   */
  private static String prepareRequestedContainer(
      DockerClient dockerClient, DockerExecutorSettings settings) throws InterruptedException {
    // this requires network access.  Once complete, "docker image ls" will show the downloaded
    // image
    fetchImageIfMissing(
        dockerClient, settings.limits.containerSettings.containerImage, settings.fetchTimeout);

    // build and start the container.  Once complete, "docker container ls" will show the started
    // container
    String containerId = createContainer(dockerClient, settings);
    dockerClient.startContainerCmd(containerId).exec();

    // copy files into it
    populateContainer(dockerClient, containerId, settings.execDir);

    // container is ready for running actions
    return containerId;
  }

  /**
   * @brief Fetch the user requested image for running the action.
   * @details The image will not be fetched if it already exists.
   * @param dockerClient Client used to interact with docker.
   * @param imageName The name of the image to fetch.
   * @param fetchTimeout When to timeout on fetching the container image.
   */
  private static void fetchImageIfMissing(
      DockerClient dockerClient, String imageName, Duration fetchTimeout)
      throws InterruptedException {
    if (!isLocalImagePresent(dockerClient, imageName)) {
      dockerClient
          .pullImageCmd(imageName)
          .exec(new PullImageResultCallback())
          .awaitCompletion(fetchTimeout.getSeconds(), TimeUnit.SECONDS);
    }
  }

  /**
   * @brief Check to see if the image was already available.
   * @details Checking to see if the image is already available can avoid having to re-fetch it.
   * @param dockerClient Client used to interact with docker.
   * @param imageName The name of the image to check for.
   * @return Whether or not the container image is already present.
   * @note Suggested return identifier: isPresent.
   */
  private static boolean isLocalImagePresent(DockerClient dockerClient, String imageName) {
    // Check if image is already downloaded. Is this the most efficient way? It would be better to
    // not use exceptions for control flow.
    try {
      dockerClient.inspectImageCmd(imageName).exec();
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * @brief Get all the host paths that should be populated into the container.
   * @details Paths with use docker's copy archive API.
   * @param execDir The execution root of the action.
   * @return Paths to copy into the container.
   * @note Suggested return identifier: paths.
   */
  private static List<Path> getPopulatePaths(Path execDir) {
    List<Path> paths = new ArrayList<>();
    paths.add(execDir);
    paths.addAll(Utils.getSymbolicLinkReferences(execDir));
    return paths;
  }

  /**
   * @brief Populate the container as needed by copying files into it.
   * @details This may or may not be necessary depending on mounts / volumes.
   * @param dockerClient Client used to interact with docker.
   * @param containerId The ID of the container.
   * @param execDir Client used to interact with docker.
   */
  private static void populateContainer(
      DockerClient dockerClient, String containerId, Path execDir) {
    for (Path path : getPopulatePaths(execDir)) {
      copyPathIntoContainer(dockerClient, containerId, path);
    }
  }

  /**
   * @brief Copies the file or directory into the container.
   * @details Copies all folder descendants.
   * @param dockerClient Client used to interact with docker.
   * @param containerId The ID of the container.
   * @param path Path to copy into container.
   */
  private static void copyPathIntoContainer(
      DockerClient dockerClient, String containerId, Path path) {
    CopyArchiveToContainerCmd cmd = dockerClient.copyArchiveToContainerCmd(containerId);
    cmd.withDirChildrenOnly(true);
    cmd.withNoOverwriteDirNonDir(false);
    cmd.withHostResource(path.toAbsolutePath().toString());
    cmd.withRemotePath(path.toAbsolutePath().toString());
    cmd.exec();
  }

  /**
   * @brief Get the exit code of the action that was executed inside the container.
   * @details Docker stores the exit code after the execution and it can be queried with an execId.
   * @param dockerClient Client used to interact with docker.
   * @param execId The ID of the execution.
   * @param resultBuilder The results to populate.
   */
  private static void extractExitCode(
      DockerClient dockerClient, String execId, ActionResult.Builder resultBuilder) {
    InspectExecCmd inspectExecCmd = dockerClient.inspectExecCmd(execId);
    InspectExecResponse response = inspectExecCmd.exec();
    resultBuilder.setExitCode(response.getExitCodeLong().intValue());
  }

  /**
   * @brief Extract information from the container after the action ran.
   * @details This can include exit code, output artifacts, and various docker information.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @param containerId The ID of the container.
   * @param execId The ID of the execution.
   * @param resultBuilder The results to populate.
   */
  private static void extractInformationFromContainer(
      DockerClient dockerClient,
      DockerExecutorSettings settings,
      String containerId,
      String execId,
      ActionResult.Builder resultBuilder)
      throws IOException {
    extractExitCode(dockerClient, execId, resultBuilder);
    copyOutputsOutOfContainer(dockerClient, settings, containerId);
  }

  /**
   * @brief Copies action outputs out of the container.
   * @details The outputs are known by the operation context.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @param containerId The ID of the container.
   */
  private static void copyOutputsOutOfContainer(
      DockerClient dockerClient, DockerExecutorSettings settings, String containerId)
      throws IOException {
    for (String outputFile : settings.executionContext.command.getOutputFilesList()) {
      Path outputPath = settings.execDir.resolve(outputFile);
      copyFileOutOfContainer(dockerClient, containerId, outputPath);
    }
    for (String outputDir : settings.executionContext.command.getOutputDirectoriesList()) {
      Path outputDirPath = settings.execDir.resolve(outputDir);
      outputDirPath.toFile().mkdirs();
    }
  }

  /**
   * @brief Delete the container.
   * @details Forces container deletion.
   * @param dockerClient Client used to interact with docker.
   * @param containerId The ID of the container.
   */
  private static void cleanUpContainer(DockerClient dockerClient, String containerId) {
    try {
      dockerClient.removeContainerCmd(containerId).withRemoveVolumes(true).withForce(true).exec();
    } catch (Exception e) {
      log.log(Level.SEVERE, "couldn't shutdown container: ", e);
    }
  }

  /**
   * @brief Assuming the container is already created and properly populated/mounted with data, this
   *     can be used to spawn an action inside of it.
   * @details The stdout / stderr of the action execution are populated to the results.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @param containerId The ID of the container.
   * @param resultBuilder The results to populate.
   * @return The ID of the container execution.
   * @note Suggested return identifier: execId.
   */
  private static String runActionInsideContainer(
      DockerClient dockerClient,
      DockerExecutorSettings settings,
      String containerId,
      ActionResult.Builder resultBuilder)
      throws InterruptedException {
    // decide command to run
    ExecCreateCmd execCmd = dockerClient.execCreateCmd(containerId);
    execCmd.withWorkingDir(settings.execDir.toAbsolutePath().toString());
    execCmd.withAttachStderr(true);
    execCmd.withAttachStdout(true);
    execCmd.withCmd(settings.arguments.toArray(new String[0]));
    String execId = execCmd.exec().getId();
    // execute command (capture stdout / stderr)
    ExecStartCmd execStartCmd = dockerClient.execStartCmd(execId);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    execStartCmd.exec(new ExecStartResultCallback(out, err)).awaitCompletion();
    // store results
    resultBuilder.setStdoutRaw(ByteString.copyFromUtf8(out.toString()));
    resultBuilder.setStderrRaw(ByteString.copyFromUtf8(err.toString()));

    return execId;
  }

  /**
   * @brief Create a docker container for the action to run in.
   * @details We can use a separate container per action or keep containers alive and re-use them.
   * @param dockerClient Client used to interact with docker.
   * @param settings Settings used to perform action execition.
   * @return The created container id.
   * @note Suggested return identifier: containerId.
   */
  private static String createContainer(
      DockerClient dockerClient, DockerExecutorSettings settings) {
    // prepare command
    CreateContainerCmd createContainerCmd =
        dockerClient.createContainerCmd(settings.limits.containerSettings.containerImage);
    createContainerCmd.withAttachStderr(true);
    createContainerCmd.withAttachStdout(true);
    createContainerCmd.withTty(true);
    createContainerCmd.withHostConfig(getHostConfig(settings.execDir));
    createContainerCmd.withEnv(MapUtils.envMapToList(settings.envVars));
    createContainerCmd.withNetworkDisabled(!settings.limits.containerSettings.network);
    createContainerCmd.withStopTimeout((int) settings.timeout.getSeconds());
    // run container creation and log any warnings
    CreateContainerResponse response = createContainerCmd.exec();
    if (response.getWarnings().length != 0) {
      log.log(Level.WARNING, Arrays.toString(response.getWarnings()));
    }
    // container is ready and started
    return response.getId();
  }

  /**
   * @brief Create a host config used for container creation.
   * @details This can determine container mounts and volumes.
   * @param execDir The execution root of the action.
   * @return A docker host configuration.
   * @note Suggested return identifier: hostConfig.
   */
  private static HostConfig getHostConfig(Path execDir) {
    HostConfig config = new HostConfig();
    mountExecRoot(config, execDir);
    return config;
  }

  /**
   * @brief Add paths needed to mount the exec root.
   * @details These are added to the host config.
   * @param config Docker host configuration to be populated.
   * @param execDir The execution root of the action.
   */
  private static void mountExecRoot(HostConfig config, Path execDir) {
    // decide paths
    List<Path> paths = new ArrayList<>();
    paths.add(Paths.get("/" + execDir.subpath(0, 1)));
    paths.add(execDir);
    paths.addAll(Utils.getSymbolicLinkReferences(execDir));
    // mount paths needed for the execution root
    List<Bind> binds = new ArrayList<>();
    for (Path path : paths) {
      binds.add(
          new Bind(path.toAbsolutePath().toString(), new Volume(path.toAbsolutePath().toString())));
    }

    config.withBinds(binds);
  }

  /**
   * @brief Copy the given file out of the container to the same host path.
   * @details The file is extracted as a tar and deserialized.
   * @param dockerClient Client used to interact with docker.
   * @param containerId The container id.
   * @param path The file to extract out of the container.
   */
  private static void copyFileOutOfContainer(
      DockerClient dockerClient, String containerId, Path path) throws IOException {
    try {
      CopyArchiveFromContainerCmd cmd =
          dockerClient.copyArchiveFromContainerCmd(containerId, path.toString());
      cmd.withHostPath(path.toString());
      cmd.withResource(path.toString());

      try (TarArchiveInputStream tarStream = new TarArchiveInputStream(cmd.exec())) {
        Utils.unTar(tarStream, new File(path.toString()));
      }
    } catch (Exception e) {
      log.log(Level.WARNING, "Could not extract file from container: ", e);
    }
  }
}
