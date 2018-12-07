package build.buildfarm.proxy.http;

import com.google.bytestream.ByteStreamProto.WriteRequest;
import io.grpc.stub.StreamObserver;

interface WriteObserver extends Write, StreamObserver<WriteRequest> {

}
