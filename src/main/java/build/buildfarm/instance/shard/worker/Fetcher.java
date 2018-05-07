package build.buildfarm.instance.shard.worker;

import com.google.devtools.remoteexecution.v1test.Digest;
import com.google.protobuf.ByteString;

public interface Fetcher {
  ByteString fetchBlob(Digest blobDigest);
}
