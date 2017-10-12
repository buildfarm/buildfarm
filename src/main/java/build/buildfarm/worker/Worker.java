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

package build.buildfarm.worker;

import build.buildfarm.common.Digests;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.v1test.CASInsertionControl;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.devtools.remoteexecution.v1test.Action;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.Command;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.Directory;
import com.google.devtools.remoteexecution.v1test.DirectoryNode;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata;
import com.google.devtools.remoteexecution.v1test.ExecuteResponse;
import com.google.devtools.remoteexecution.v1test.FileNode;
import com.google.devtools.remoteexecution.v1test.OutputFile;
import com.google.devtools.remoteexecution.v1test.Platform;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker {
  public static final Logger logger = Logger.getLogger(Worker.class.getName());
  private final Instance instance;
  private final WorkerConfig config;
  private final Path root;
  private final Path cacheDir;

  private static final OutputStream nullOutputStream = new OutputStream() {
    @Override
    public void write(int b) {
    }

    @Override
    public void write(byte[] b) {
    }

    @Override
    public void write(byte[] b, int off, int len) {
    }
  };

  private static class Poller implements Runnable {
    private final Duration period;
    private final BooleanSupplier poll;
    private boolean running;

    public Poller(Duration period, BooleanSupplier poll) {
      this.period = period;
      this.poll = poll;
      running = true;
    }

    @Override
    public synchronized void run() {
      while (running) {
        try {
          this.wait(
              period.getSeconds() * 1000 + period.getNanos() / 1000000,
              period.getNanos() % 1000000);
          if (running) {
            // FP interface with distinct returns, do not memoize!
            running = poll.getAsBoolean();
          }
        } catch (InterruptedException ex) {
          running = false;
        }
      }
    }

    public synchronized void stop() {
      running = false;
      this.notify();
    }
  }

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Path getValidRoot(WorkerConfig config) throws ConfigurationException {
    String rootValue = config.getRoot();
    // Have to handle empty string here because all later APIs happily ignore
    // empty strings (WTF)
    if (Strings.isNullOrEmpty(rootValue)) {
        throw new ConfigurationException("root value in config missing");
    }
    return Paths.get(rootValue);
  }

  private static Path getValidCasCacheDirectory(WorkerConfig config, Path root) throws ConfigurationException {
    String casCacheValue = config.getCasCacheDirectory();
    // Have to handle empty string here because all later APIs happily ignore
    // empty strings (WTF)
    if (Strings.isNullOrEmpty(casCacheValue)) {
        throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(casCacheValue);
  }

  public Worker(WorkerConfig config) throws ConfigurationException{
    this.config = config;
    root = getValidRoot(config);
    cacheDir = getValidCasCacheDirectory(config, root);
    instance = new StubInstance(
        config.getInstanceName(),
        createChannel(config.getOperationQueue()));
  }

  public void start() {
    try {
      Files.createDirectories(root);
      Files.createDirectories(cacheDir);
    } catch(IOException ex) {
      ex.printStackTrace();
      return;
    }
    for (;;) {
      instance.match(config.getPlatform(), config.getRequeueOnFailure(), (operation) -> {
        try {
          execute(operation);
          return true;
        } catch(Exception ex) {
          ex.printStackTrace();
          return false;
        }
      });
    }
  }

  private void execute(Operation operation)
      throws InvalidProtocolBufferException, IOException,
      CacheNotFoundException, InterruptedException {
    final String operationName = operation.getName();
    Poller poller = new Poller(config.getOperationPollPeriod(), () -> instance.pollOperation(
        operationName,
        ExecuteOperationMetadata.Stage.QUEUED));
    new Thread(poller).start();

    ExecuteOperationMetadata metadata =
        operation.getMetadata().unpack(ExecuteOperationMetadata.class);
    Action action = Action.parseFrom(instance.getBlob(metadata.getActionDigest()));

    Path execDir = root.resolve(operation.getName());
    Files.createDirectories(execDir);

    try {
      /* set up inputs */
      /* we should update the operation status at this point to indicate input fetches have begun */
      if (!fetchInputs(execDir, action.getInputRootDigest()) ||
          !verifyOutputLocations(execDir, action.getOutputFilesList(), action.getOutputDirectoriesList())) {
        poller.stop();
        return;
      }

      ExecuteOperationMetadata executingMetadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.EXECUTING)
          .build();

      operation = operation.toBuilder()
          .setMetadata(Any.pack(executingMetadata))
          .build();

      poller.stop();

      if (!instance.putOperation(operation)) {
        return;
      }

      poller = new Poller(config.getOperationPollPeriod(), () -> instance.pollOperation(
          operationName,
          ExecuteOperationMetadata.Stage.EXECUTING));
      new Thread(poller).start();

      Command command = Command.parseFrom(instance.getBlob(action.getCommandDigest()));

      Duration timeout;
      if (action.hasTimeout()) {
        timeout = action.getTimeout();
      } else {
        timeout = null;
      }

      /* execute command */
      ActionResult.Builder resultBuilder = executeCommand(execDir, command, timeout, metadata.getStdoutStreamName(), metadata.getStderrStreamName());

      ImmutableList.Builder<ByteString> contents = new ImmutableList.Builder<>();
      if (resultBuilder.getExitCode() == 0) {
        CASInsertionControl control = config.getFileCasControl();
        for (String outputFile : action.getOutputFilesList()) {
          Path outputPath = execDir.resolve(outputFile);
          if (!Files.exists(outputPath)) {
            continue;
          }

          InputStream inputStream = Files.newInputStream(outputPath);
          ByteString content = ByteString.readFrom(inputStream);
          inputStream.close();
          OutputFile.Builder outputFileBuilder = resultBuilder.addOutputFilesBuilder()
              .setPath(outputFile)
              .setIsExecutable(Files.isExecutable(outputPath));
          boolean withinLimit = content.size() <= config.getFileCasControl().getLimit();
          if (withinLimit) {
            outputFileBuilder.setContent(content);
          }
          if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
              (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
            contents.add(content);

            // FIXME make this happen with putAllBlobs
            Digest outputDigest = Digests.computeDigest(content);
            outputFileBuilder.setDigest(outputDigest);
          }
        }

        instance.putAllBlobs(contents.build());
      }

      ActionResult result = resultBuilder.build();
      if (!action.getDoNotCache()) {
        instance.putActionResult(metadata.getActionDigest(), result);
      }

      metadata = metadata.toBuilder()
          .setStage(ExecuteOperationMetadata.Stage.COMPLETED)
          .build();

      operation = operation.toBuilder()
          .setDone(true)
          .setMetadata(Any.pack(metadata))
          .setResponse(Any.pack(ExecuteResponse.newBuilder()
              .setResult(result)
              .setCachedResult(false)
              .build()))
          .build();

      poller.stop();

      instance.putOperation(operation);
    } finally {
      // guard against IOException in particular
      poller.stop(); // no effect if already stopped

      /* cleanup */
      removeDirectory(execDir);
    }
  }

  private boolean linkInputs(Path execDir, Digest inputRoot, Map<Digest, Directory> directoriesIndex) throws IOException {
    Directory directory = directoriesIndex.get(inputRoot);

    for (FileNode fileNode : directory.getFilesList()) {
      Digest digest = fileNode.getDigest();
      Path fileCachePath = cacheDir.resolve(String.format(
          "%s_%d%s",
          digest.getHash(),
          digest.getSizeBytes(),
          fileNode.getIsExecutable() ? "_exec" : ""));
      boolean createFile = !Files.exists(fileCachePath);
      if (createFile) {
        OutputStream outputStream = Files.newOutputStream(fileCachePath);
        instance.getBlob(digest).writeTo(outputStream);
        outputStream.close();
      }
      if (createFile || Files.isExecutable(fileCachePath) != fileNode.getIsExecutable()) {
        ImmutableSet.Builder<PosixFilePermission> perms = new ImmutableSet.Builder<PosixFilePermission>()
          .add(PosixFilePermission.OWNER_READ);
        if (fileNode.getIsExecutable()) {
          perms.add(PosixFilePermission.OWNER_EXECUTE);
        }
        Files.setPosixFilePermissions(fileCachePath, perms.build());
      }
      Files.createLink(execDir.resolve(fileNode.getName()), fileCachePath);
    }

    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      Digest digest = directoryNode.getDigest();
      Path dirPath = execDir.resolve(directoryNode.getName());
      Files.createDirectory(dirPath);
      linkInputs(dirPath, directoryNode.getDigest(), directoriesIndex);
    }

    return true;
  }

  private boolean fetchInputs(Path execDir, Digest inputRoot) throws IOException {
    ImmutableList.Builder<Directory> directories = new ImmutableList.Builder<>();
    String pageToken = "";

    do {
      pageToken = instance.getTree(inputRoot, config.getTreePageSize(), pageToken, directories);
    } while (!pageToken.isEmpty());

    Set<Digest> directoryDigests = new HashSet<>();
    ImmutableMap.Builder<Digest, Directory> directoriesIndex = new ImmutableMap.Builder<>();
    for (Directory directory : directories.build()) {
      Digest directoryDigest = Digests.computeDigest(directory);
      if (!directoryDigests.add(directoryDigest)) {
        continue;
      }
      directoriesIndex.put(directoryDigest, directory);
    }

    return linkInputs(execDir, inputRoot, directoriesIndex.build());
  }

  private boolean verifyOutputLocations(
      Path execDir,
      Iterable<String> outputFiles,
      Iterable<String> outputDirs) throws IOException {
    for (String outputFile : outputFiles) {
      Path outputFilePath = execDir.resolve(outputFile);
      Files.createDirectories(outputFilePath.getParent());
    }

    for (String outputDir : outputDirs) {
      logger.info("outputDir: " + outputDir);
      return false;
    }

    return true;
  }

  private ActionResult.Builder executeCommand(
      Path execDir,
      Command command,
      Duration timeout,
      String stdoutStreamName,
      String stderrStreamName)
      throws IOException, InterruptedException {
    ProcessBuilder processBuilder =
        new ProcessBuilder(command.getArgumentsList())
            .directory(execDir.toAbsolutePath().toFile());

    Map<String, String> environment = processBuilder.environment();
    environment.clear();
    for (Command.EnvironmentVariable environmentVariable : command.getEnvironmentVariablesList()) {
      environment.put(environmentVariable.getName(), environmentVariable.getValue());
    }

    OutputStream stdoutSink = null, stderrSink = null;

    if (stdoutStreamName != null && !stdoutStreamName.isEmpty() && config.getStreamStdout()) {
      stdoutSink = instance.getStreamOutput(stdoutStreamName);
    } else {
      stdoutSink = nullOutputStream;
    }
    if (stderrStreamName != null && !stderrStreamName.isEmpty() && config.getStreamStderr()) {
      stderrSink = instance.getStreamOutput(stderrStreamName);
    } else {
      stderrSink = nullOutputStream;
    }

    long startNanoTime = System.nanoTime();
    int exitValue = -1;
    Process process;
    try {
      process = processBuilder.start();
      process.getOutputStream().close();
    } catch(IOException ex) {
      // again, should we do something else here??
      ActionResult.Builder resultBuilder = ActionResult.newBuilder()
          .setExitCode(exitValue);
      return resultBuilder;
    }

    InputStream stdoutStream = process.getInputStream();
    InputStream stderrStream = process.getErrorStream();

    ByteStringSinkReader stdoutReader = new ByteStringSinkReader(
        process.getInputStream(), stdoutSink);
    ByteStringSinkReader stderrReader = new ByteStringSinkReader(
        process.getErrorStream(), stderrSink);

    Thread stdoutReaderThread = new Thread(stdoutReader);
    Thread stderrReaderThread = new Thread(stderrReader);
    stdoutReaderThread.start();
    stderrReaderThread.start();

    boolean doneWaiting = false;
    if (timeout == null) {
      exitValue = process.waitFor();
    } else {
      while (!doneWaiting) {
        long timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
        long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
        if (remainingNanoTime > 0) {
          if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
            exitValue = process.exitValue();
            doneWaiting = true;
          }
        } else {
          process.destroyForcibly();
          process.waitFor(100, TimeUnit.MILLISECONDS); // fair trade, i think
          doneWaiting = true;
        }
      }
    }
    if (!stdoutReader.isComplete()) {
      stdoutReaderThread.interrupt();
    }
    stdoutReaderThread.join();
    if (!stderrReader.isComplete()) {
      stderrReaderThread.interrupt();
    }
    stderrReaderThread.join();
    ActionResult.Builder resultBuilder = ActionResult.newBuilder()
        .setExitCode(exitValue);
    ByteString stdoutRaw = stdoutReader.getData();
    if (stdoutRaw.size() > 0) {
      CASInsertionControl control = config.getStdoutCasControl();
      boolean withinLimit = stdoutRaw.size() <= control.getLimit();
      if (withinLimit) {
        resultBuilder.setStdoutRaw(stdoutRaw);
      }
      if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
          (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
        resultBuilder.setStdoutDigest(instance.putBlob(stdoutRaw));
      }
    }
    ByteString stderrRaw = stderrReader.getData();
    if (stderrRaw.size() > 0) {
      CASInsertionControl control = config.getStderrCasControl();
      boolean withinLimit = stderrRaw.size() <= control.getLimit();
      if (withinLimit) {
        resultBuilder.setStderrRaw(stderrRaw);
      }
      if (control.getPolicy() == CASInsertionControl.Policy.ALWAYS_INSERT ||
          (!withinLimit && control.getPolicy() == CASInsertionControl.Policy.INSERT_ABOVE_LIMIT)) {
        resultBuilder.setStderrDigest(instance.putBlob(stderrRaw));
      }
    }
    return resultBuilder;
  }

  private void removeDirectory(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Decodes the given byte array assumed to be encoded with ISO-8859-1 encoding (isolatin1).
   */
  private static char[] convertFromLatin1(byte[] content) {
    char[] latin1 = new char[content.length];
    for (int i = 0; i < latin1.length; i++) { // yeah, latin1 is this easy! :-)
      latin1[i] = (char) (0xff & content[i]);
    }
    return latin1;
  }

  private static WorkerConfig toWorkerConfig(InputStream inputStream) throws IOException {
    WorkerConfig.Builder builder = WorkerConfig.newBuilder();
    String data = new String(convertFromLatin1(ByteStreams.toByteArray(inputStream)));
    TextFormat.merge(data, builder);
    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    Path configPath = Paths.get(args[0]);
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      Worker worker = new Worker(toWorkerConfig(configInputStream));
      configInputStream.close();
      worker.start();
    }
  }
}
