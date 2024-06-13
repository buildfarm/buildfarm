package build.buildfarm.common;

import static com.google.common.truth.Truth.assertThat;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZstdCompressingInputStreamTest {
  @Test
  public void testSkip() throws IOException {
    String blobToSkip = "AAAAA"; // 5 bytes
    String blobToRead = "BBBBBBBBBBBBBBB"; // 15 bytes
    String blob = blobToSkip + blobToRead; // 20 bytes
    InputStream inputStream = new ByteArrayInputStream(blob.getBytes());
    ZstdCompressingInputStream zstdIn = new ZstdCompressingInputStream(inputStream);
    assertThat(zstdIn.skip(blobToSkip.length())).isEqualTo(blobToSkip.length());

    byte[] buf = new byte[20]; // compressed data can be larger than original data
    zstdIn.read(buf);
    String readBlob = new String(Zstd.decompress(buf, blobToRead.length()), StandardCharsets.UTF_8);
    assertThat(readBlob).isEqualTo(blobToRead);
  }
}
