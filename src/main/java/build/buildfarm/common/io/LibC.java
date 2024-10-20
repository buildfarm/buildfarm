// Copyright 2020 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.io;

import jnr.ffi.Pointer;

/* in order to call libc functions directly using ffi,
   we must specify the routines here in a lib c interface.
   FFI types are used as a shared data type between Java/C.
*/
public interface LibC {
  int syscall(int number, Object... args);

  Pointer opendir(String s);

  @SuppressWarnings("UnusedReturnValue")
  int closedir(Pointer dir);

  Pointer readdir(Pointer d);
}
