package build.buildfarm.common;

import jnr.ffi.Pointer;

/* in order to call libc functions directly using ffi,
   we must specify the routines here in a lib c interface.
   FFI types are used as a shared data type between Java/C.
*/
public interface LibC {
  int syscall(int number, Object... args);

  int puts(String s);

  Pointer opendir(String s);

  int closedir(Pointer dir);

  Pointer readdir(Pointer d);
}
