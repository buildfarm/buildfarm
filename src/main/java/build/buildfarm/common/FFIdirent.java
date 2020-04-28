package build.buildfarm.common;

import jnr.ffi.Struct;

public class FFIdirent extends Struct {

  private static final int MAX_NAME_LEN = 255;

  public FFIdirent(jnr.ffi.Runtime runtime) {
    super(runtime);
  }

  public java.lang.String getName() {
    return d_name.toString();
  }

  /* data can be extracted from readdir() */
  public final Signed64 d_ino = new Signed64();
  public final Signed64 d_off = new Signed64();
  public final Unsigned16 d_reclen = new Unsigned16();
  public final Unsigned8 d_type = new Unsigned8();
  public final AsciiString d_name = new AsciiString(MAX_NAME_LEN);
}
