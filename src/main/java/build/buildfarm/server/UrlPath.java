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

package build.buildfarm.server;

import build.buildfarm.common.Digests;
import com.google.common.collect.Iterables;
import com.google.devtools.remoteexecution.v1test.Digest;
import java.util.Arrays;

public class UrlPath {
  public enum ServiceType {
    Blob(3),
    UploadBlob(5),
    OperationStream(4);

    private final int minComponentLength;
    private ServiceType(int minComponentLength) {
      this.minComponentLength = minComponentLength;
    }
  }

  public static ServiceType detectServiceType(String resourceName) {
    // TODO: Replace this with proper readonly parser should this become
    // a bottleneck
    String[] components = resourceName.split("/");
    
    // Keep following checks ordered by descending minComponentLength
    if (components.length >= ServiceType.UploadBlob.minComponentLength && isUploadBlob(components)) {
      return ServiceType.UploadBlob;
    } else if (components.length >= ServiceType.OperationStream.minComponentLength && isOperationStream(components)) {
      return ServiceType.OperationStream;
    } else if (components.length >= ServiceType.Blob.minComponentLength && isBlob(components)) {
      return ServiceType.Blob;
    }

    throw new IllegalArgumentException("Url path not recognized: " + resourceName);
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
      throws IllegalArgumentException {
    String[] components = resourceName.split("/");
    String hash = components[components.length - 2];
    long size = Long.parseLong(components[components.length - 1]);
    return Digests.buildDigest(hash, size);
  }

  public static Digest parseUploadBlobDigest(String resourceName)
      throws IllegalArgumentException {
    String[] components = resourceName.split("/");
    String hash = components[components.length - 2];
    long size = Long.parseLong(components[components.length - 1]);
    return Digests.buildDigest(hash, size);
  }

  public static String parseOperationStream(String resourceName) {
    String[] components = resourceName.split("/");
    return String.join("/", Arrays.asList(components).subList(components.length - 4, components.length));
  }
}

