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
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.grpc.TracingMetadataUtils;
import build.buildfarm.instance.Instance;
import build.buildfarm.v1test.Digest;
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

  // The `:` character is not permitted in an HTTP header name.
  // These are use specifically to indicate that headers should only be applied to specific URLs
  public static final String QUALIFIER_HTTP_HEADER_URL_PREFIX = "http_header_url:";

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
      String name,
      String value,
      DigestFunction.Value digestFunction,
      FetchQualifiers.Builder fetchQualifiers) {
    switch (name) {
      case QUALIFIER_CANONICAL_ID -> fetchQualifiers.setCanonicalId(value);
      case QUALIFIER_CHECKSUM_SRI ->
          fetchQualifiers.setDigest(parseChecksumSRI(value, digestFunction));
      case String s when s.startsWith(QUALIFIER_HTTP_HEADER_PREFIX) ->
          fetchQualifiers.putHeaders(s.substring(QUALIFIER_HTTP_HEADER_PREFIX.length()), value);
      case String s when s.startsWith(QUALIFIER_HTTP_HEADER_URL_PREFIX) ->
          fetchQualifiers.putHeaders(s.substring(QUALIFIER_HTTP_HEADER_URL_PREFIX.length()), value);
      default -> {
        // ignore unknown qualifiers
      }
    }
  }

  private static FetchQualifiers parseQualifiers(
      Iterable<Qualifier> qualifiers, DigestFunction.Value digestFunction) {
    FetchQualifiers.Builder fetchQualifiers = FetchQualifiers.newBuilder();
    for (Qualifier qualifier : qualifiers) {
      parseQualifier(qualifier.getName(), qualifier.getValue(), digestFunction, fetchQualifiers);
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

    FetchQualifiers qualifiers =
        parseQualifiers(request.getQualifiersList(), request.getDigestFunction());

    Digest expectedDigest = qualifiers.getDigest();

    // TODO consider doing something with QUALIFIER_CANONICAL_ID

    build.bazel.remote.execution.v2.Digest.Builder result =
        build.bazel.remote.execution.v2.Digest.newBuilder();
    if (!expectedDigest.equals(Digest.getDefaultInstance())
        && instance.containsBlob(expectedDigest, result, requestMetadata)) {
      responseObserver.onNext(FetchBlobResponse.newBuilder().setBlobDigest(result.build()).build());
      responseObserver.onCompleted();
      return;
    }

    addCallback(
        instance.fetchBlob(
            request.getUrisList(), qualifiers.getHeadersMap(), expectedDigest, requestMetadata),
        new FutureCallback<>() {
          @Override
          public void onSuccess(Digest actualDigest) {
            log.log(
                Level.INFO,
                format(
                    "fetch blob succeeded: %s inserted into CAS",
                    DigestUtil.toString(actualDigest)));
            responseObserver.onNext(
                FetchBlobResponse.newBuilder()
                    .setBlobDigest(DigestUtil.toDigest(actualDigest))
                    .setDigestFunction(actualDigest.getDigestFunction())
                    .build());
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

  private static build.buildfarm.v1test.Digest parseChecksumSRI(
      String checksum, DigestFunction.Value digestFunction) {
    String[] components = checksum.split("-");
    if (components.length != 2) {
      throw Status.INVALID_ARGUMENT
          .withDescription(format("Invalid checksum format '%s'", checksum))
          .asRuntimeException();
    }
    String hashFunction = components[0];
    String encodedDigest = components[1];
    DigestUtil digestUtil = DigestUtil.forHash(hashFunction.toUpperCase());
    if (digestUtil == null) {
      throw Status.INVALID_ARGUMENT
          .withDescription(format("Unrecognized digest function '%s'", hashFunction))
          .asRuntimeException();
    }
    if (digestFunction != DigestFunction.Value.UNKNOWN
        && digestFunction != digestUtil.getDigestFunction()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              format(
                  "Mismatched digest function '%s' and checksum.sri '%s'",
                  digestFunction, hashFunction))
          .asRuntimeException();
    }
    byte[] hash = BaseEncoding.base64().decode(encodedDigest);
    return digestUtil.build(BaseEncoding.base16().lowerCase().encode(hash), -1);
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
