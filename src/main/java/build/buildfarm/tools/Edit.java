// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.bazel.remote.execution.v2.Compressor.Value.IDENTITY;
import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.instance.Utils.getBlob;
import static build.buildfarm.instance.Utils.putBlob;
import static java.util.concurrent.TimeUnit.HOURS;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.Tree;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Stack;

class Edit {
  private static Tree fetchTree(Instance instance, Digest rootDigest) {
    Tree.Builder tree = Tree.newBuilder();
    String pageToken = Instance.SENTINEL_PAGE_TOKEN;

    do {
      pageToken = instance.getTree(rootDigest, 1024, pageToken, tree);
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));

    return tree.build();
  }

  private static Directory adjustDirEntry(Directory directory, String name, Digest digest) {
    Directory.Builder builder = directory.toBuilder();
    builder.clearFiles();
    for (FileNode fileNode : directory.getFilesList()) {
      if (fileNode.getName().equals(name)) {
        fileNode = fileNode.toBuilder().setDigest(DigestUtil.toDigest(digest)).build();
      }
      builder.addFiles(fileNode);
    }
    return builder.build();
  }

  private static Directory adjustDirChild(Directory directory, String name, Digest digest) {
    Directory.Builder builder = directory.toBuilder();
    builder.clearDirectories();
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      if (directoryNode.getName().equals(name)) {
        directoryNode = directoryNode.toBuilder().setDigest(DigestUtil.toDigest(digest)).build();
      }
      builder.addDirectories(directoryNode);
    }
    return builder.build();
  }

  private static Directory getDir(Directory directory, String name, Map<String, Directory> index) {
    for (DirectoryNode directoryNode : directory.getDirectoriesList()) {
      if (directoryNode.getName().equals(name)) {
        return index.get(directoryNode.getDigest().getHash());
      }
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    String host = args[0];
    String instanceName = args[1];
    Digest actionDigest = DigestUtil.parseDigest(args[2]);

    ManagedChannel channel = createChannel(host);
    Instance instance = new StubInstance(instanceName, channel);
    RequestMetadata metadata = RequestMetadata.getDefaultInstance();

    Action action = Action.parseFrom(getBlob(instance, IDENTITY, actionDigest, metadata));
    Digest commandDigest = DigestUtil.fromDigest(action.getCommandDigest(), actionDigest.getDigestFunction());
    Command command = Command.parseFrom(getBlob(instance, IDENTITY, commandDigest, metadata));

    // bazel-out/tda4-toliman-opt/bin/autonomy/planning/trajectory_generator/task/task_onboard_binary_cc_binary-0.params
    Stack<Directory> path = new Stack<>();
    Tree tree = fetchTree(instance, DigestUtil.fromDigest(action.getInputRootDigest(), actionDigest.getDigestFunction()));

    Stack<String> names = new Stack<>();
    Directory directory = tree.getDirectoriesMap().get(action.getInputRootDigest().getHash());
    path.push(directory);

    String filePath = "bazel-out/tda4-toliman-opt/bin/autonomy/planning/trajectory_generator/task";

    for (String component : filePath.split("/")) {
      names.push(component);
      directory = getDir(directory, component, tree.getDirectoriesMap());
      path.push(directory);
    }

    DigestUtil digestUtil = new DigestUtil(DigestUtil.HashFunction.get(actionDigest.getDigestFunction()));

    ByteString content;
    Digest contentDigest;
    try (InputStream in = Files.newInputStream(Paths.get("params"))) {
      content = ByteString.readFrom(in);
      contentDigest = digestUtil.compute(content);
    }
    putBlob(instance, IDENTITY, contentDigest, content, 1, HOURS, metadata);
    System.out.println(String.format("Adjusting file entry to be %s", DigestUtil.toString(contentDigest)));
    Directory container = adjustDirEntry(path.pop(), "task_onboard_binary_cc_binary-0.params", contentDigest);
    while (!path.isEmpty()) {
      Digest containerDigest = digestUtil.compute(container);
      putBlob(instance, IDENTITY, containerDigest, container.toByteString(), 1, HOURS, metadata);
      String name = names.pop();
      System.out.println(String.format("Adjusting dir entry for %s to be %s", name, DigestUtil.toString(containerDigest)));
      container = adjustDirChild(path.pop(), name, containerDigest);
    }
    Digest containerDigest = digestUtil.compute(container);
    putBlob(instance, IDENTITY, containerDigest, container.toByteString(), 1, HOURS, metadata);
    System.out.println(String.format("Adjusting root to be %s", DigestUtil.toString(containerDigest)));
    action = action.toBuilder()
        .setInputRootDigest(DigestUtil.toDigest(containerDigest))
        .build();

    Property coresProperty = Property.newBuilder()
        .setName("cores")
        .setValue("10")
        .build();

    command = command.toBuilder()
        .setPlatform(command.getPlatform().toBuilder().addProperties(coresProperty))
        .build();
    action = action.toBuilder()
        .setPlatform(action.getPlatform().toBuilder().addProperties(coresProperty))
        .build();

    ByteString actionBlob = action.toByteString();
    actionDigest = digestUtil.compute(actionBlob);
    ByteString commandBlob = command.toByteString();
    commandDigest = digestUtil.compute(commandBlob);
    putBlob(instance, IDENTITY, actionDigest, actionBlob, 1, HOURS, metadata);
    putBlob(instance, IDENTITY, commandDigest, commandBlob, 1, HOURS, metadata);
    System.out.println(DigestUtil.toString(actionDigest));

    instance.stop();
  }
}
