package build.buildfarm.server;

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
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.FutureCallback;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.logging.Level;
import java.util.logging.Logger;

class FetchService extends FetchImplBase {
  public static final Logger logger = Logger.getLogger(ActionCacheService.class.getName());

  private final Instance instance;

  FetchService(Instance instance) {
    this.instance = instance;
  }

  @Override
  public void fetchBlob(
      FetchBlobRequest request, StreamObserver<FetchBlobResponse> responseObserver) {
    fetchBlob(instance, request, responseObserver);
  }

  private void fetchBlob(
      Instance instance,
      FetchBlobRequest request,
      StreamObserver<FetchBlobResponse> responseObserver) {
    Digest expectedDigest = null;
    RequestMetadata requestMetadata = TracingMetadataUtils.fromCurrentContext();
    if (request.getQualifiersCount() == 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Empty qualifier list").asRuntimeException();
    }
    for (Qualifier qualifier : request.getQualifiersList()) {
      String name = qualifier.getName();
      if (name.equals("checksum.sri")) {
        expectedDigest = parseChecksumSRI(qualifier.getValue());
        Digest.Builder result = Digest.newBuilder();
        if (instance.containsBlob(expectedDigest, result, requestMetadata)) {
          responseObserver.onNext(
              FetchBlobResponse.newBuilder().setBlobDigest(result.build()).build());
        }
      } else {
        throw Status.INVALID_ARGUMENT
            .withDescription(format("Invalid qualifier '%s'", name))
            .asRuntimeException();
      }
    }
    if (request.getUrisCount() != 0) {
      addCallback(
          instance.fetchBlob(request.getUrisList(), expectedDigest, requestMetadata),
          new FutureCallback<Digest>() {
            @Override
            public void onSuccess(Digest actualDigest) {
              logger.log(
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
              logger.log(Level.SEVERE, "fetch blob failed", t);
              responseObserver.onError(t);
            }
          },
          directExecutor());
    } else {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("Empty uris list").asRuntimeException());
    }
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
    logger.log(
        Level.SEVERE,
        "fetchDirectory: "
            + request.toString()
            + ",\n metadata: "
            + TracingMetadataUtils.fromCurrentContext());
    responseObserver.onError(Status.UNIMPLEMENTED.asException());
  }
}
