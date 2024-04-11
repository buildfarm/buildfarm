// Copyright 2022 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common.resources;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * @class Resources
 * @brief A parser/formatter of information encoded in Bytestream's API resource names per the
 *     Remote Exception API.
 * @details Google's bytestream API is used for REAPI clients to make resource requests against the
 *     CAS (content-addressable storage). The ByteStream API contains ReadRequest and WriteRequest
 *     types which carry an attribute known as "resource name." The "resource name" is a URI that
 *     must be parsed in order to derive specific resource information defined by REAPI. REAPI
 *     describes how this information will be encoded in the "resource name." The following
 *     utilities are for parsing "resource name" and extracting the information into easy to work
 *     with types.
 */
public class ResourceParser {
  /**
   * @field RESOURCE_SEPARATOR
   * @brief Path separator to use when parsing.
   * @details Part of REAPI's format of resource name.
   */
  private static final String RESOURCE_SEPARATOR = "/";

  private static final String UNCOMPRESSED_BLOBS_KEYWORD = "blobs";

  /**
   * @field COMPRESSED_BLOBS_KEYWORD
   * @brief Path keyword for identifying the blob is compressed.
   * @details Used to detect URI format based on compression.
   */
  private static final String COMPRESSED_BLOBS_KEYWORD = "compressed-blobs";

  /**
   * @field KEYWORDS
   * @brief A list of keywords to their corresponding types.
   * @details This lookup table is used for parsing resource_names.
   */
  private static final HashMap<String, Resource.TypeCase> KEYWORDS = keywordResourceMap();

  /**
   * @brief Categorize the resource type by analyzing a resource name URI.
   * @details The URI is parsed to identify one of the possible Resource types it is in reference
   *     to.
   * @param resourceName The resource name URI defined by REAPI.
   * @return The resource type derived from the URI.
   * @note Suggested return identifier: resourceType.
   */
  public static Resource.TypeCase getResourceType(String resourceName) {
    // Each "resource name" starts with an optional "instance name" which might consist of multiple
    // path segments Each "resource name" might also end with "optional metadata" which might also
    // consist of multiple path segments In order to parse the resource type effectively and without
    // relying on a segment index, REAPI gives the following guarantee: "To simplify parsing, the
    // instance_name segments cannot equal any of the following keywords: `blobs`, `uploads`,
    // `actions`, `actionResults`, `operations`,`capabilities` or `compressed-blobs`." Therefore we
    // have to scan linearly to identify the resource type by the first known keyword.
    // https://github.com/bazelbuild/remote-apis/blob/04784f4a830cc0df1f419a492cde9fc323f728db/build/bazel/remote/execution/v2/remote_execution.proto#L215

    // Find the first instance of such a keyword.
    String type =
        Stream.of(tokenize(resourceName))
            .filter(segment -> KEYWORDS.containsKey(segment))
            .findFirst()
            .orElse("");

    // Return the resource type. If no resource type was identified, return null.
    return KEYWORDS.get(type);
  }

  /**
   * @brief Parse the resource name into a specific type.
   * @details Assumes the URI is already in the upload blob format.
   * @param resourceName The resource name URI defined by REAPI.
   * @return The resource type derived from the URI.
   * @note Suggested return identifier: request.
   */
  public static UploadBlobRequest parseUploadBlobRequest(String resourceName) {
    // Find the index of the keyword.  This will give us an initial index to extract information
    // from.
    String[] segments = tokenize(resourceName);
    MutableInt index = startIndex(segments, Resource.TypeCase.UPLOAD_BLOB_REQUEST);

    // Extract each of the following segments:
    // `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}{/optional_metadata}` or
    // `{instance_name}/uploads/{uuid}/compressed-blobs/{compressor}/{uncompressed_hash}/{uncompressed_size}{/optional_metadata}`
    UploadBlobRequest.Builder builder = UploadBlobRequest.newBuilder();
    builder.setInstanceName(asResourcePath(previousSegments(segments, index)));
    index.increment();
    builder.setUuid(segments[index.getAndIncrement()]);
    builder.setBlob(parseBlobInformation(segments, index));
    if (index.intValue() < segments.length) {
      builder.setMetadata(asResourcePath(remainingSegments(segments, index)));
    }
    return builder.build();
  }

  private static void addCompressorName(
      ImmutableList.Builder<String> resource, Compressor.Value compression) {
    if (compression == Compressor.Value.IDENTITY) {
      resource.add(UNCOMPRESSED_BLOBS_KEYWORD);
    } else {
      resource.add(COMPRESSED_BLOBS_KEYWORD);
      switch (compression) {
        case IDENTITY:
          break;
        case ZSTD:
          resource.add("zstd");
          break;
        case DEFLATE:
          resource.add("deflate");
          break;
        default:
          throw new IllegalArgumentException("Unknown Compressor: " + compression);
      }
    }
  }

  private static void addDigestResource(ImmutableList.Builder<String> resource, Digest digest) {
    resource.add(digest.getHash());
    resource.add(String.format("%d", digest.getSizeBytes()));
  }

  private static void addBlobResource(
      ImmutableList.Builder<String> resource, BlobInformation blob) {
    addCompressorName(resource, blob.getCompressor());
    addDigestResource(resource, blob.getDigest());
  }

  /**
   * @brief Produce the resource name for a ByteStream ReadRequest
   * @param request The request to be encoded into a resource name.
   * @return The resource name derived from the request.
   * @note Suggested return identifier: resourceName.
   */
  public static String downloadResourceName(DownloadBlobRequest request) {
    ImmutableList.Builder resource = ImmutableList.builder();
    if (!request.getInstanceName().isEmpty()) {
      resource.add(request.getInstanceName());
    }
    addBlobResource(resource, request.getBlob());
    return asResourcePath(resource.build());
  }

  /**
   * @brief Produce the resource name for a ByteStream WriteRequest
   * @param request The request to be encoded into a resource name.
   * @return The resource name derived from the request.
   * @note Suggested return identifier: resourceName.
   */
  public static String uploadResourceName(UploadBlobRequest request) {
    ImmutableList.Builder resource = ImmutableList.builder();
    if (!request.getInstanceName().isEmpty()) {
      resource.add(request.getInstanceName());
    }
    resource.add("uploads");
    resource.add(request.getUuid());
    addBlobResource(resource, request.getBlob());
    if (!request.getMetadata().isEmpty()) {
      resource.add(request.getMetadata());
    }
    return asResourcePath(resource.build());
  }

  /**
   * @brief Parse the resource name into a specific type.
   * @details Assumes the URI is already in the download blob format.
   * @param resourceName The resource name URI defined by REAPI.
   * @return The resource type derived from the URI.
   * @note Suggested return identifier: request.
   */
  public static DownloadBlobRequest parseDownloadBlobRequest(String resourceName) {
    // Find the index of the keyword.  This will give us an initial index to extract information
    // from.
    String[] segments = tokenize(resourceName);
    MutableInt index = startIndex(segments, Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);

    // Extract each of the following segments: `{instance_name}/blobs/{hash}/{size}` or
    // `{instance_name}/compressed-blobs/{compressor}/{uncompressed_hash}/{uncompressed_size}`
    DownloadBlobRequest.Builder builder = DownloadBlobRequest.newBuilder();
    builder.setInstanceName(asResourcePath(previousSegments(segments, index)));
    builder.setBlob(parseBlobInformation(segments, index));
    return builder.build();
  }

  /**
   * @brief A mapping of REAPI keywords to their respective resource types.
   * @details This lookup table can be used for parsing.
   * @return A lookup table for keyword to resource type.
   * @note Suggested return identifier: keywordMap.
   */
  private static HashMap<String, Resource.TypeCase> keywordResourceMap() {
    HashMap<String, Resource.TypeCase> keywords = new HashMap<>();
    keywords.put("blobs", Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);
    keywords.put(COMPRESSED_BLOBS_KEYWORD, Resource.TypeCase.DOWNLOAD_BLOB_REQUEST);
    keywords.put("uploads", Resource.TypeCase.UPLOAD_BLOB_REQUEST);
    keywords.put("operations", Resource.TypeCase.STREAM_OPERATION_REQUEST);
    return keywords;
  }

  /**
   * @brief Tokenize the resource name URI.
   * @details A simple split should be fine.
   * @param resourceName The resource name URI defined by REAPI.
   * @return A tokenized resource name.
   * @note Suggested return identifier: uRITokens.
   */
  private static String[] tokenize(String resourceName) {
    return resourceName.split(RESOURCE_SEPARATOR);
  }

  /**
   * @brief The index to start parsing resource name segments from.
   * @details Because the beginning and end of resource names can be multi-segment, discovering this
   *     index let's us properly parse out segments.
   * @param segments The resource name URI segments.
   * @param type The type to consider when finding an index with a keyword.
   * @return The index to start parsing resource name segments from.
   * @note Suggested return identifier: startIndex.
   */
  private static MutableInt startIndex(String[] segments, Resource.TypeCase type) {
    return new MutableInt(findKeywordIndex(segments, KEYWORDS, type));
  }

  /**
   * @brief The index to start parsing resource name segments from.
   * @details Because the beginning and end of resource names can be multi-segment, discovering this
   *     index let's us properly parse out segments.
   * @param segments The resource name URI segments.
   * @param keywords Keyword to resource types.
   * @param type The type to consider when finding an index with a keyword.
   * @return The index to start parsing resource name segments from.
   * @note Suggested return identifier: keywordIndex.
   */
  private static int findKeywordIndex(
      String[] segments, HashMap<String, Resource.TypeCase> keywords, Resource.TypeCase type) {
    return IntStream.range(0, segments.length)
        .filter(i -> keywords.get(segments[i]) == type)
        .findFirst()
        .orElse(0);
  }

  /**
   * @brief Parse blob information from the URI segments.
   * @details Progrsses the parser while extracting blob information.
   * @param segments The resource name URI segments.
   * @param index The current parser index.
   * @return Parsed blob information.
   * @note Suggested return identifier: blobInformation.
   */
  private static BlobInformation parseBlobInformation(String[] segments, MutableInt index) {
    BlobInformation.Builder builder = BlobInformation.newBuilder();
    boolean isCompressed = segments[index.getAndIncrement()].equals(COMPRESSED_BLOBS_KEYWORD);
    if (isCompressed) {
      builder.setCompressor(
          Compressor.Value.valueOf(segments[index.getAndIncrement()].toUpperCase()));
    } else {
      builder.setCompressor(Compressor.Value.IDENTITY);
    }
    String hash = segments[index.getAndIncrement()];
    String size = segments[index.getAndIncrement()];
    builder.setDigest(parseDigest(hash, size));
    return builder.build();
  }

  /**
   * @brief Parse digest information into type.
   * @details Size interpreted as long.
   * @param hash The extracted hash value.
   * @param size The extracted hash size value.
   * @return Parsed digest information.
   * @note Suggested return identifier: digest.
   */
  private static Digest parseDigest(String hash, String size) {
    return Digest.newBuilder().setHash(hash).setSizeBytes(Long.parseLong(size)).build();
  }

  /**
   * @brief Combine a list of path segments into the resource_name path format.
   * @details Used on a set of previously tokenized segments.
   * @param pathSegments Path segments to make a resource path.
   * @return Combined segments.
   * @note Suggested return identifier: resourcePath.
   */
  private static String asResourcePath(Iterable<String> pathSegments) {
    return String.join(RESOURCE_SEPARATOR, pathSegments);
  }

  /**
   * @brief Collect a list of the previous segments from the given index.
   * @details Used for instance name extraction.
   * @param segments Path segments to make a resource path.
   * @param index Index to extract from.
   * @return Extracted segments from given index.
   * @note Suggested return identifier: extractedSegments.
   */
  private static List<String> previousSegments(String[] segments, MutableInt index) {
    return Arrays.asList(Arrays.copyOfRange(segments, 0, index.intValue()));
  }

  /**
   * @brief Collect a list of the remaining segments from the given index.
   * @details Used for metadata extraction.
   * @param segments Path segments to make a resource path.
   * @param index Index to extract from.
   * @return Remaining segments from given index.
   * @note Suggested return identifier: remainingSegments.
   */
  private static List<String> remainingSegments(String[] segments, MutableInt index) {
    return Arrays.asList(Arrays.copyOfRange(segments, index.intValue(), segments.length));
  }
}
