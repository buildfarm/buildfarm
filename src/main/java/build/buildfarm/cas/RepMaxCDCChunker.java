package build.buildfarm.cas;

import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.common.DigestUtil;
import java.io.InputStream;
import java.util.Iterator;

public class RepMaxCDCChunker implements Iterator<Blob> {
  private final DigestUtil digestUtil;
  private final InputStream in;

  public RepMaxCDCChunker(DigestUtil digestUtil, InputStream in) {
    this.digestUtil = digestUtil;
    this.in = in;
  }

  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Blob next() {
    throw new UnsupportedOperationException();
  }
}
