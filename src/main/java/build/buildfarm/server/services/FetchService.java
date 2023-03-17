package build.buildfarm.server.services;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.asset.v1.FetchBlobRequest;
import build.bazel.remote.asset.v1.FetchBlobResponse;
import build.bazel.remote.asset.v1.FetchDirectoryRequest;
import build.bazel.remote.asset.v1.FetchDirectoryResponse;
import build.bazel.remote.asset.v1.FetchGrpc.FetchImplBase;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.Collectors;

import lombok.extern.java.Log;

@Log
public class FetchService extends FetchImplBase {
  private final Instance instance;

  public FetchService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void fetchBlob(
      FetchBlobRequest request, StreamObserver<FetchBlobResponse> responseObserver) {
    try {
      fetchBlob(instance, request, responseObserver);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void fetchBlob(
      Instance instance,
      FetchBlobRequest request,
      StreamObserver<FetchBlobResponse> responseObserver)
      throws InterruptedException {
    Digest expectedDigest = null;
    RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
    if (request.getQualifiersCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Empty qualifier list").asRuntimeException();
    }

    Map<String,String> qualifiers = request.getQualifiersList().stream().collect(Collectors.toMap(q -> q.getName(), q -> q.getValue()));
    if ( ! qualifiers.containsKey("checksum.sri")) {
      throw Status.INVALID_ARGUMENT.withDescription("Missing checksum.sri qualifier").asRuntimeException();
    }

    Map<String, String> authHeaders = new HashMap<>();
    if (qualifiers.containsKey("bazel.auth_headers") ) {
      String checkAuthHeaders = qualifiers.get("bazel.auth_headers");
      authHeaders = parseAuthHeaders(checkAuthHeaders);
    }

    String checksumSri = qualifiers.get("checksum.sri");
    expectedDigest = parseChecksumSRI(checksumSri);
    Digest.Builder result = Digest.newBuilder();
    if (instance.containsBlob(expectedDigest, result, requestMetadata)) {
      responseObserver.onNext(
              FetchBlobResponse.newBuilder().setBlobDigest(result.build()).build());
      responseObserver.onCompleted();
      return;
    }

    if (request.getUrisCount() != 0) {
      addCallback(
          instance.fetchBlob(request.getUrisList(), authHeaders, expectedDigest, requestMetadata),
          new FutureCallback<Digest>() {
            @Override
            public void onSuccess(Digest actualDigest) {
              log.log(
                  Level.INFO,
                  format(
                      "fetch blob succeeded: %s inserted into CAS",
                      DigestUtil.toString(actualDigest)));
              responseObserver.onNext(
                  FetchBlobResponse.newBuilder().setBlobDigest(actualDigest).build());
              responseObserver.onCompleted();
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public void onFailure(Throwable t) {
              // handle NoSuchFileException
              log.log(Level.SEVERE, "fetch blob failed", t);
              responseObserver.onError(t);
            }
          },
          directExecutor());
    } else {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("Empty uris list").asRuntimeException());
    }
  }

  private Map<String, String> parseAuthHeaders(String rawJson) {
    Map<String, String> authHeaders = new HashMap<>();
    ObjectMapper jsonMapper = new ObjectMapper();
    TypeReference<Map<String, Map<String, String>>> typeRef =
            new TypeReference<Map<String, Map<String, String>>>() {};
    try {
      jsonMapper.readValue(rawJson, typeRef).forEach((key, value) -> {
        if (value.containsKey("Authorization")) {
          authHeaders.put(key, value.get("Authorization"));
        }
      });
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return authHeaders;
  }

  private Digest parseChecksumSRI(String checksum) {
    String[] components = checksum.split("-");
    if (components.length != 2) {
      throw Status.INVALID_ARGUMENT
          .withDescription(format("Invalid checksum format '%s'", checksum))
          .asRuntimeException();
    }
    String hashFunction = components[0];
    String encodedDigest = components[1];
    DigestUtil digestUtil = DigestUtil.forHash(hashFunction.toUpperCase());
    return digestUtil.build(BaseEncoding.base64().decode(encodedDigest), -1);
  }

  @Override
  public void fetchDirectory(
      FetchDirectoryRequest request, StreamObserver<FetchDirectoryResponse> responseObserver) {
    log.log(
        Level.SEVERE,
        "fetchDirectory: "
            + request.toString()
            + ",\n metadata: "
            + TracingMetadataUtils.fromCurrentContext());
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }
}
