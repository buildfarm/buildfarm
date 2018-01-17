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

import build.buildfarm.common.Encoding;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.WorkerConfig;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.Any;
import com.google.protobuf.TextFormat;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
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

  public Worker(WorkerConfig config) throws ConfigurationException {
    this.config = config;

    /* configuration validation */
    root = getValidRoot(config);
    Path casCacheDirectory = getValidCasCacheDirectory(config, root);

    /* initialization */
    instance = new StubInstance(
        config.getInstanceName(),
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
        config.getCasCacheMaxSizeBytes());
  }

  public void start() throws InterruptedException {
    try {
      Files.createDirectories(root);
      fileCache.start();
    } catch(IOException ex) {
      ex.printStackTrace();
      return;
    }

    PipelineStage errorStage = new ReportResultStage.NullStage(); /* ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(this, errorStage);
    PipelineStage executeActionStage = new ExecuteActionStage(this, reportResultStage, errorStage);
    reportResultStage.setInput(executeActionStage);
    PipelineStage inputFetchStage = new InputFetchStage(this, executeActionStage, errorStage);
    executeActionStage.setInput(inputFetchStage);
    PipelineStage matchStage = new MatchStage(this, inputFetchStage, errorStage);
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

  public static void removeDirectory(Path directory) throws IOException {
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

  private static WorkerConfig toWorkerConfig(InputStream inputStream, WorkerOptions options) throws IOException {
    WorkerConfig.Builder builder = WorkerConfig.newBuilder();
    String data = new String(Encoding.convertFromLatin1(ByteStreams.toByteArray(inputStream)));
    TextFormat.merge(data, builder);
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
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      Worker worker = new Worker(toWorkerConfig(configInputStream, parser.getOptions(WorkerOptions.class)));
      configInputStream.close();
      worker.start();
    }
  }
}
