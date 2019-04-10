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

import com.google.common.collect.Iterables;
import build.bazel.remote.execution.v2.Digest;
import java.util.Arrays;
import java.util.UUID;

// FIXME move this to Resources, use a oneof protobuf to describe them
public class UrlPath {
  public static class InvalidResourceNameException extends Exception {
    private final String resourceName;

    public InvalidResourceNameException(String resourceName, String message) {
      super(message);
      this.resourceName = resourceName;
    }

    public InvalidResourceNameException(String resourceName, String message, Throwable cause) {
      super(message, cause);
      this.resourceName = resourceName;
    }

    public String getResourceName() {
      return resourceName;
    }

    @Override
    public String getMessage() {
      return String.format(
          "%s: %s",
          resourceName,
          super.getMessage());
    }
  }

  public enum ResourceOperation {
    Blob(3),
    UploadBlob(5),
    OperationStream(4);

    private final int minComponentLength;
    private ResourceOperation(int minComponentLength) {
      this.minComponentLength = minComponentLength;
    }
  }

  public static ResourceOperation detectResourceOperation(String resourceName)
      throws InvalidResourceNameException {
    // TODO: Replace this with proper readonly parser should this become
    // a bottleneck
    String[] components = resourceName.split("/");

    // Keep following checks ordered by descending minComponentLength
    if (components.length >= ResourceOperation.UploadBlob.minComponentLength && isUploadBlob(components)) {
      return ResourceOperation.UploadBlob;
    } else if (components.length >= ResourceOperation.OperationStream.minComponentLength && isOperationStream(components)) {
      return ResourceOperation.OperationStream;
    } else if (components.length >= ResourceOperation.Blob.minComponentLength && isBlob(components)) {
      return ResourceOperation.Blob;
    }

    throw new InvalidResourceNameException(resourceName, "Url path not recognized");
  }

  private static boolean isBlob(String[] components) {
    // {instance_name=**}/blobs/{hash}/{size}
    return components[components.length - 3].equals("blobs");
  }

  private static boolean isUploadBlob(String[] components) {
    // {instance_name=**}/uploads/{uuid}/blobs/{hash}/{size}
    return components[components.length - 3].equals("blobs") &&
      components[components.length - 5].equals("uploads");
  }

  private static boolean isOperationStream(String[] components) {
    // {instance_name=**}/operations/{uuid}/streams/{stream}
    return components[components.length - 2].equals("streams") &&
        components[components.length - 4].equals("operations");
  }

  public static String fromOperationsCollectionName(String operationsCollectionName) {
    // {instance_name=**}/operations
    String[] components = operationsCollectionName.split("/");
    return String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 1));
  }

  public static String fromOperationName(String operationName) {
    // {instance_name=**}/operations/{uuid}
    String[] components = operationName.split("/");
    if (components.length < 2) {
      return "";
    }
    return String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 2));
  }

  public static String fromOperationStream(String operationStream) {
    // {instance_name=**}/operations/{uuid}/streams/{stream}
    String[] components = operationStream.split("/");
    return String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 4));
  }

  public static String fromBlobName(String blobName) {
    // {instance_name=**}/blobs/{hash}/{size}
    String[] components = blobName.split("/");
    return String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 3));
  }

  public static String fromUploadBlobName(String uploadBlobName) {
    // {instance_name=**}/uploads/{uuid}/blobs/{hash}/{size}
    String[] components = uploadBlobName.split("/");
    return String.join(
        "/", Iterables.limit(
            Arrays.asList(components),
            components.length - 5));
  }

  public static Digest parseBlobDigest(String resourceName)
      throws InvalidResourceNameException {
    String[] components = resourceName.split("/");
    if (components.length < 2) {
      throw new InvalidResourceNameException(resourceName, "invalid blob name");
    }
    String hash = components[components.length - 2];
    long size;
    try {
      size = Long.parseLong(components[components.length - 1]);
    } catch (NumberFormatException e) {
      throw new InvalidResourceNameException(resourceName, e.getMessage(), e);
    }
    if (size <= 0) {
      throw new InvalidResourceNameException(
          resourceName,
          String.format("upload size invalid: %d", size));
    }
    return Digest.newBuilder()
        .setHash(hash)
        .setSizeBytes(size)
        .build();
  }

  // FIXME all upload parsing tools need to accomodate for the following
  // from the remote execution specification:
  //
  // The `resource_name` may optionally have a trailing filename
  // (or other metadata) for a client to use if it is storing URLs, as in
  // `{instance}/uploads/{uuid}/blobs/{hash}/{size}/foo/bar/baz.cc`. Anything
  // after the `size` is ignored.
  public static UUID parseUploadBlobUUID(String resourceName)
      throws InvalidResourceNameException {
    // ... `uuid` is a version 4 UUID generated by the client
    String[] components = resourceName.split("/");
    try {
      return UUID.fromString(components[components.length - 4]);
    } catch (IllegalArgumentException e) {
      throw new InvalidResourceNameException(resourceName, e.getMessage(), e);
    }
  }

  public static Digest parseUploadBlobDigest(String resourceName)
      throws InvalidResourceNameException {
    String[] components = resourceName.split("/");
    String hash = components[components.length - 2];
    long size;
    try {
      size = Long.parseLong(components[components.length - 1]);
    } catch (NumberFormatException e) {
      throw new InvalidResourceNameException(resourceName, e.getMessage(), e);
    }
    if (size <= 0) {
      throw new InvalidResourceNameException(
          resourceName,
          String.format("upload size invalid: %d", size));
    }
    return Digest.newBuilder()
        .setHash(hash)
        .setSizeBytes(size)
        .build();
  }

  public static String parseOperationStream(String resourceName)
      throws InvalidResourceNameException {
    String[] components = resourceName.split("/");
    if (components.length <= 4) {
      throw new InvalidResourceNameException(resourceName, "invalid operation stream name");
    }
    return String.join("/", Arrays.asList(components).subList(components.length - 4, components.length));
  }
}

