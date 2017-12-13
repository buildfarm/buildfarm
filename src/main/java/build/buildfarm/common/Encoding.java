// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.common;

public class Encoding {
  /**
   * Decodes the given byte array assumed to be encoded with ISO-8859-1 encoding (isolatin1).
   */
  public static char[] convertFromLatin1(byte[] content) {
    char[] latin1 = new char[content.length];
    for (int i = 0; i < latin1.length; i++) { // yeah, latin1 is this easy! :-)
      latin1[i] = (char) (0xff & content[i]);
    }
    return latin1;
  }
}

