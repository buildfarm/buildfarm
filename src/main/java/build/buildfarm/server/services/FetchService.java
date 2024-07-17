package build.buildfarm.server.services;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;

import build.bazel.remote.asset.v1.FetchBlobRequest;
import build.bazel.remote.asset.v1.FetchBlobResponse;
import build.bazel.remote.asset.v1.FetchDirectoryRequest;
import build.bazel.remote.asset.v1.FetchDirectoryResponse;
import build.bazel.remote.asset.v1.FetchGrpc.FetchImplBase;
import build.bazel.remote.asset.v1.Qualifier;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.FetchQualifiers;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class FetchService extends FetchImplBase {
  private final Instance instance;

  // The `Qualifier::name` field uses well-known string keys to attach arbitrary
  // key-value metadata to download requests. These are the qualifier names
  // supported by Bazel.
  public static final String QUALIFIER_CHECKSUM_SRI = "checksum.sri";
  public static final String QUALIFIER_CANONICAL_ID = "bazel.canonical_id";

  // The `:` character is not permitted in an HTTP header name. So, we use it to
  // delimit the qualifier prefix which denotes an HTTP header qualifer from the
  // header name itself.
  public static final String QUALIFIER_HTTP_HEADER_PREFIX = "http_header:";

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

  private static void parseQualifier(
      String name, String value, FetchQualifiers.Builder fetchQualifiers) {
    switch (name) {
      case QUALIFIER_CANONICAL_ID -> fetchQualifiers.setCanonicalId(value);
      case QUALIFIER_CHECKSUM_SRI -> fetchQualifiers.setDigest(parseChecksumSRI(value));
      case String s when s.startsWith(QUALIFIER_HTTP_HEADER_PREFIX) ->
          fetchQualifiers.putHeaders(s.substring(QUALIFIER_HTTP_HEADER_PREFIX.length()), value);
      default -> {
        // ignore unknown qualifiers
      }
    }
  }

  private static FetchQualifiers parseQualifiers(Iterable<Qualifier> qualifiers) {
    FetchQualifiers.Builder fetchQualifiers = FetchQualifiers.newBuilder();
    for (Qualifier qualifier : qualifiers) {
      parseQualifier(qualifier.getName(), qualifier.getValue(), fetchQualifiers);
    }
    return fetchQualifiers.build();
  }

  private void fetchBlob(
      Instance instance,
      FetchBlobRequest request,
      StreamObserver<FetchBlobResponse> responseObserver)
      throws InterruptedException {
    RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();

    if (request.getUrisCount() == 0) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("Empty uris list").asRuntimeException());
      return;
    }

    FetchQualifiers qualifiers = parseQualifiers(request.getQualifiersList());

    Digest expectedDigest = qualifiers.getDigest();

    // TODO consider doing something with QUALIFIER_CANONICAL_ID

    Digest.Builder result = Digest.newBuilder();
    if (!expectedDigest.equals(Digest.getDefaultInstance())
        && instance.containsBlob(expectedDigest, result, requestMetadata)) {
      responseObserver.onNext(FetchBlobResponse.newBuilder().setBlobDigest(result.build()).build());
      responseObserver.onCompleted();
      return;
    }

    addCallback(
        instance.fetchBlob(
            request.getUrisList(), qualifiers.getHeadersMap(), expectedDigest, requestMetadata),
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
  }

  private static Digest parseChecksumSRI(String checksum) {
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
