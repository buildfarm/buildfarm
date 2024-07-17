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

package build.buildfarm.common.grpc;

import build.bazel.remote.execution.v2.RequestMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.MetadataUtils;
import javax.annotation.Nullable;

/** Utility functions to handle Metadata for remote Grpc calls. */
public final class TracingMetadataUtils {
  private TracingMetadataUtils() {}

  private static final Context.Key<RequestMetadata> CONTEXT_KEY =
      Context.key("remote-grpc-metadata");

  @VisibleForTesting
  public static final Metadata.Key<RequestMetadata> METADATA_KEY =
      ProtoUtils.keyForProto(RequestMetadata.getDefaultInstance());

  /** Fetches a {@link RequestMetadata} defined on the current context, or the default instance. */
  public static RequestMetadata fromCurrentContext() {
    RequestMetadata metadata = CONTEXT_KEY.get();
    if (metadata == null) {
      metadata = RequestMetadata.getDefaultInstance();
    }
    return metadata;
  }

  /**
   * Extracts a {@link RequestMetadata} from a {@link Metadata} and returns it if it exists. If it
   * does not exist, returns {@code null}.
   */
  public static @Nullable RequestMetadata requestMetadataFromHeaders(Metadata headers) {
    return headers.get(METADATA_KEY);
  }

  public static ClientInterceptor attachMetadataInterceptor(RequestMetadata metadata) {
    Metadata headers = new Metadata();
    headers.put(METADATA_KEY, metadata);
    return MetadataUtils.newAttachHeadersInterceptor(headers);
  }

  /** GRPC interceptor to add logging metadata to the GRPC context. */
  public static class ServerHeadersInterceptor implements ServerInterceptor {
    @Override
    @SuppressWarnings("PMD.GenericsNaming")
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      RequestMetadata meta = requestMetadataFromHeaders(headers);
      if (meta == null) {
        meta = RequestMetadata.getDefaultInstance();
      }
      Context ctx = Context.current().withValue(CONTEXT_KEY, meta);
      return Contexts.interceptCall(ctx, call, headers, next);
    }
  }
}
