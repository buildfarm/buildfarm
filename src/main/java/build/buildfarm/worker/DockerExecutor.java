// Copyright 2021 The Bazel Authors. All rights reserved.
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
import build.buildfarm.worker.resources.ResourceLimits;
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
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.rpc.Code;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

class DockerExecutor {
  private static final Logger logger = Logger.getLogger(DockerExecutor.class.getName());

  public static Code runActionWithDocker(
      OperationContext operationContext,
      Path execDir,
      ResourceLimits limits,
      Duration timeout,
      List<String> arguments,
      Map<String, String> envVars,
      ActionResult.Builder resultBuilder)
      throws InterruptedException, IOException {
    // construct docker client
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    // prepare container for execution
    String containerId = prepareRequestedContainer(dockerClient, execDir, limits, timeout, envVars);

    // run action inside the container
    String execId =
        runActionInsideContainer(dockerClient, containerId, execDir, arguments, resultBuilder);

    extractInformationFromContainer(
        dockerClient, operationContext, containerId, execId, execDir, resultBuilder);

    cleanUpContainer(dockerClient, containerId);

    return Code.OK;
  }

  private static void extractInformationFromContainer(
      DockerClient dockerClient,
      OperationContext operationContext,
      String containerId,
      String execId,
      Path execDir,
      ActionResult.Builder resultBuilder)
      throws IOException {
    // extract action's exit code
    InspectExecCmd inspectExecCmd = dockerClient.inspectExecCmd(execId);
    InspectExecResponse response = inspectExecCmd.exec();
    resultBuilder.setExitCode(response.getExitCodeLong().intValue());

    // export action outputs
    copyOutputsOutOfContainer(dockerClient, operationContext, containerId, execDir);
  }

  private static void cleanUpContainer(DockerClient dockerClient, String containerId) {
    // clean up container
    try {
      dockerClient.removeContainerCmd(containerId).withRemoveVolumes(true).withForce(true).exec();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "couldn't shutdown container: ", e);
    }
  }

  private static String prepareRequestedContainer(
      DockerClient dockerClient,
      Path execDir,
      ResourceLimits limits,
      Duration timeout,
      Map<String, String> envVars)
      throws InterruptedException {
    // get the image (network access)
    fetchImageIfMissing(dockerClient, limits.containerSettings.containerImage);

    // build container
    String containerId = createContainer(dockerClient, execDir, limits, timeout, envVars);

    // start container
    dockerClient.startContainerCmd(containerId).exec();

    // copy files into container
    copyFilesIntoContainer(dockerClient, containerId, execDir);
    copyCacheIntoContainer(dockerClient, containerId, execDir);

    // container is ready for running actions
    return containerId;
  }

  private static String runActionInsideContainer(
      DockerClient dockerClient,
      String containerId,
      Path execDir,
      List<String> arguments,
      ActionResult.Builder resultBuilder)
      throws InterruptedException {
    // decide command to run
    ExecCreateCmd execCmd = dockerClient.execCreateCmd(containerId);
    execCmd.withWorkingDir(execDir.toAbsolutePath().toString());
    execCmd.withAttachStderr(true);
    execCmd.withAttachStdout(true);
    execCmd.withCmd(arguments.toArray(new String[0]));

    String execId = execCmd.exec().getId();

    // execute command
    ExecStartCmd execStartCmd = dockerClient.execStartCmd(execId);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    execStartCmd.exec(new ExecStartResultCallback(out, err)).awaitCompletion();

    // store results
    resultBuilder.setStdoutRaw(ByteString.copyFromUtf8(out.toString()));
    resultBuilder.setStderrRaw(ByteString.copyFromUtf8(err.toString()));

    return execId;
  }

  private static String createContainer(
      DockerClient dockerClient,
      Path execDir,
      ResourceLimits limits,
      Duration timeout,
      Map<String, String> envVars)
      throws InterruptedException {
    CreateContainerCmd createContainerCmd =
        dockerClient.createContainerCmd(limits.containerSettings.containerImage);

    // prepare command
    createContainerCmd.withAttachStderr(true);
    createContainerCmd.withAttachStdout(true);
    createContainerCmd.withTty(true);
    createContainerCmd.withHostConfig(getHostConfig(execDir));
    createContainerCmd.withEnv(envMapToList(envVars));
    createContainerCmd.withNetworkDisabled(!limits.containerSettings.network);
    createContainerCmd.withStopTimeout((int) timeout.getSeconds());

    // run container creation and log any warnings
    CreateContainerResponse response = createContainerCmd.exec();
    if (response.getWarnings().length != 0) {
      logger.log(Level.WARNING, Arrays.toString(response.getWarnings()));
    }

    // container is ready to be started
    return response.getId();
  }

  private static HostConfig getHostConfig(Path execDir) {
    HostConfig config = new HostConfig();
    mountExecRoot(config, execDir);
    return config;
  }

  private static void mountExecRoot(HostConfig config, Path execDir) {
    List<Bind> binds = new ArrayList<>();

    // mount paths needed for the execution root
    String execDirRoot = "/" + execDir.subpath(0, 1).toString();
    binds.add(new Bind(execDirRoot, new Volume(execDirRoot)));

    String execDirStr = execDir.toAbsolutePath().toString();
    binds.add(new Bind(execDirStr, new Volume(execDirStr)));

    for (Path path : getSymbolicLinkReferences(execDir)) {
      binds.add(
          new Bind(path.toAbsolutePath().toString(), new Volume(path.toAbsolutePath().toString())));
    }

    config.withBinds(binds);
  }

  private static List<Path> getSymbolicLinkReferences(Path execDir) {
    List<Path> paths = new ArrayList<>();

    try {
      Files.walk(execDir, FileVisitOption.FOLLOW_LINKS)
          .forEach(
              path -> {
                if (Files.isSymbolicLink(path)) {
                  try {
                    Path reference = Files.readSymbolicLink(path);
                    paths.add(reference);
                  } catch (IOException e) {
                    logger.log(Level.WARNING, "Could not derive symbolic link: ", e);
                  }
                }
              });
    } catch (Exception e) {
      logger.log(Level.WARNING, "Could not traverse execDir: ", e);
    }

    return paths;
  }

  private static void copyCacheIntoContainer(
      DockerClient dockerClient, String containerId, Path execDir) {
    for (Path path : getSymbolicLinkReferences(execDir)) {
      copyFilesIntoContainer(dockerClient, containerId, path);
    }
  }

  private static void copyFilesIntoContainer(
      DockerClient dockerClient, String containerId, Path path) {
    CopyArchiveToContainerCmd cmd = dockerClient.copyArchiveToContainerCmd(containerId);
    cmd.withDirChildrenOnly(true);
    cmd.withNoOverwriteDirNonDir(false);
    cmd.withHostResource(path.toAbsolutePath().toString());
    cmd.withRemotePath(path.toAbsolutePath().toString());
    cmd.exec();
  }

  private static void copyOutputsOutOfContainer(
      DockerClient dockerClient,
      OperationContext operationContext,
      String containerId,
      Path execDir)
      throws IOException {
    String execDirStr = execDir.toAbsolutePath().toString();

    for (String outputFile : operationContext.command.getOutputFilesList()) {
      Path outputPath = operationContext.execDir.resolve(outputFile);
      copyFileOutOfContainer(dockerClient, containerId, outputPath);
    }
    for (String outputDir : operationContext.command.getOutputDirectoriesList()) {
      Path outputDirPath = operationContext.execDir.resolve(outputDir);
      outputDirPath.toFile().mkdirs();
    }
  }

  private static void copyFileOutOfContainer(
      DockerClient dockerClient, String containerId, Path path) throws IOException {
    CopyArchiveFromContainerCmd cmd =
        dockerClient.copyArchiveFromContainerCmd(containerId, path.toString());
    cmd.withHostPath(path.toString());
    cmd.withResource(path.toString());

    try (TarArchiveInputStream tarStream = new TarArchiveInputStream(cmd.exec())) {
      unTar(tarStream, new File(path.toString()));
    }
  }

  public static void unTar(TarArchiveInputStream tis, File destFile) throws IOException {
    TarArchiveEntry tarEntry = null;
    while ((tarEntry = tis.getNextTarEntry()) != null) {
      if (tarEntry.isDirectory()) {
        if (!destFile.exists()) {
          destFile.mkdirs();
        }
      } else {
        FileOutputStream fos = new FileOutputStream(destFile);
        IOUtils.copy(tis, fos);
        fos.close();
      }
    }
    tis.close();
  }

  private static void fetchImageIfMissing(DockerClient dockerClient, String imageName)
      throws InterruptedException {
    // pull image if we don't already have it
    if (!isLocalImagePresent(dockerClient, imageName)) {
      dockerClient
          .pullImageCmd(imageName)
          .exec(new PullImageResultCallback())
          .awaitCompletion(1, TimeUnit.MINUTES);
    }
  }

  private static boolean isLocalImagePresent(DockerClient dockerClient, String imageName) {
    // Check if image is already downloaded.
    // Is this the most efficient way?
    // It would be better to not use exceptions for control flow.
    try {
      dockerClient.inspectImageCmd(imageName).exec();
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  private static List<String> envMapToList(Map<String, String> envVars) {
    // docker configuration needs the environment variables in the format VAR=VAL
    List<String> envList = new ArrayList<>();
    for (Map.Entry<String, String> environmentVariable : envVars.entrySet()) {
      envList.add(environmentVariable.getKey() + "=" + environmentVariable.getValue());
    }

    return envList;
  }
}
