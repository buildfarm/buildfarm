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

package build.buildfarm.worker.operationqueue;

import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.BlobPathFactory;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.CASInsertionPolicy;
import build.buildfarm.v1test.WorkerConfig;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.InputStreamFactory;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.Poller;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.WorkerContext;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.devtools.remoteexecution.v1test.ExecuteOperationMetadata.Stage;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.TextFormat;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker {
  public static final Logger logger = Logger.getLogger(Worker.class.getName());
  public final Instance instance;
  public final WorkerConfig config;
  public final Path root;
  public final CASFileCache fileCache;

  private static ManagedChannel createChannel(String target) {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget(target)
            .negotiationType(NegotiationType.PLAINTEXT);
    return builder.build();
  }

  private static Path getValidRoot(WorkerConfig config) throws ConfigurationException {
    String rootValue = config.getRoot();
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }
    return Paths.get(rootValue);
  }

  private static Path getValidCasCacheDirectory(WorkerConfig config, Path root) throws ConfigurationException {
    String casCacheValue = config.getCasCacheDirectory();
    if (Strings.isNullOrEmpty(casCacheValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(casCacheValue);
  }

  private static HashFunction getValidHashFunction(WorkerConfig config) throws ConfigurationException {
    try {
      return HashFunction.get(config.getHashFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }

  public Worker(WorkerConfig config) throws ConfigurationException {
    this.config = config;

    /* configuration validation */
    root = getValidRoot(config);
    Path casCacheDirectory = getValidCasCacheDirectory(config, root);
    HashFunction hashFunction = getValidHashFunction(config);

    /* initialization */
    instance = new StubInstance(
        config.getInstanceName(),
        new DigestUtil(hashFunction),
        createChannel(config.getOperationQueue()));
    InputStreamFactory inputStreamFactory = new InputStreamFactory() {
      @Override
      public InputStream apply(Digest digest) {
        return instance.newStreamInput(instance.getBlobName(digest));
      }
    };
    fileCache = new CASFileCache(
        inputStreamFactory,
        root.resolve(casCacheDirectory),
        config.getCasCacheMaxSizeBytes(),
        instance.getDigestUtil());
  }

  public void start() throws InterruptedException {
    try {
      Files.createDirectories(root);
      fileCache.start();
    } catch(IOException ex) {
      ex.printStackTrace();
      return;
    }

    WorkerContext workerContext = new WorkerContext() {
      @Override
      public Poller createPoller(String name, String operationName, Stage stage, Runnable onFailure) {
        Poller poller = new Poller(config.getOperationPollPeriod(), () -> {
              boolean success = instance.pollOperation(operationName, stage);
              if (!success) {
                onFailure.run();
              }
              return success;
            });
        new Thread(poller).start();
        return poller;
      }

      @Override
      public DigestUtil getDigestUtil() {
        return instance.getDigestUtil();
      }

      @Override
      public void match(Predicate<Operation> onMatch) throws InterruptedException {
        instance.match(config.getPlatform(), config.getRequeueOnFailure(), onMatch);
      }

      @Override
      public  int getInlineContentLimit() {
        return config.getInlineContentLimit();
      }

      @Override
      public CASInsertionPolicy getFileCasPolicy() {
        return config.getFileCasPolicy();
      }

      @Override
      public CASInsertionPolicy getStdoutCasPolicy() {
        return config.getStdoutCasPolicy();
      }

      @Override
      public CASInsertionPolicy getStderrCasPolicy() {
        return config.getStderrCasPolicy();
      }

      @Override
      public int getExecuteStageWidth() {
        return config.getExecuteStageWidth();
      }

      @Override
      public int getTreePageSize() {
        return config.getTreePageSize();
      }

      @Override
      public boolean getLinkInputDirectories() {
        return config.getLinkInputDirectories();
      }

      @Override
      public boolean hasDefaultActionTimeout() {
        return config.hasDefaultActionTimeout();
      }

      @Override
      public boolean hasMaximumActionTimeout() {
        return config.hasMaximumActionTimeout();
      }

      @Override
      public boolean getStreamStdout() {
        return config.getStreamStdout();
      }

      @Override
      public boolean getStreamStderr() {
        return config.getStreamStderr();
      }

      @Override
      public Duration getDefaultActionTimeout() {
        return config.getDefaultActionTimeout();
      }

      @Override
      public Duration getMaximumActionTimeout() {
        return config.getMaximumActionTimeout();
      }

      @Override
      public Instance getInstance() {
        return instance;
      }

      @Override
      public ByteString getBlob(Digest digest) {
        return instance.getBlob(digest);
      }

      @Override
      public BlobPathFactory getBlobPathFactory() {
        return fileCache;
      }

      @Override
      public Path getRoot() {
        return root;
      }

      @Override
      public void removeDirectory(Path path) throws IOException {
        CASFileCache.removeDirectory(path);
      }
    };

    PipelineStage errorStage = new ReportResultStage.NullStage(); /* ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(workerContext, errorStage);
    PipelineStage executeActionStage = new ExecuteActionStage(workerContext, reportResultStage, errorStage);
    reportResultStage.setInput(executeActionStage);
    PipelineStage inputFetchStage = new InputFetchStage(workerContext, executeActionStage, errorStage);
    executeActionStage.setInput(inputFetchStage);
    PipelineStage matchStage = new MatchStage(workerContext, inputFetchStage, errorStage);
    inputFetchStage.setInput(matchStage);

    Pipeline pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 1);
    pipeline.add(inputFetchStage, 2);
    pipeline.add(executeActionStage, 3);
    pipeline.add(reportResultStage, 4);
    pipeline.start();
    pipeline.join(); // uninterruptable
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
  }

  private static WorkerConfig toWorkerConfig(Readable input, WorkerOptions options) throws IOException {
    WorkerConfig.Builder builder = WorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }

    if (!Strings.isNullOrEmpty(options.casCacheDirectory)) {
      builder.setCasCacheDirectory(options.casCacheDirectory);
    }
    return builder.build();
  }

  private static void printUsage(OptionsParser parser) {
    System.out.println("Usage: CONFIG_PATH");
    System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                                              OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) throws Exception {
    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    Worker worker;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      worker = new Worker(toWorkerConfig(new InputStreamReader(configInputStream), parser.getOptions(WorkerOptions.class)));
    }
    worker.start();
  }
}
