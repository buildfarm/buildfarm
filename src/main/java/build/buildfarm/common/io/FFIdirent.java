/**
 * Performs specialized operation based on method logic
 * @param runtime the runtime parameter
 * @return the public result
 */
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
  public final UTF8String d_name = new UTF8String(MAX_NAME_LEN);
}
