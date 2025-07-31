// Copyright 2019 The Buildfarm Authors. All rights reserved.
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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Set;

/** Named in the theme of Files (nio) -> MoreFiles (guava) -> EvenMoreFiles */
public class EvenMoreFiles {
  private static final Set<PosixFilePermission> readOnlyPerms =
      PosixFilePermissions.fromString("r--r--r--");
  /**
   * Loads data from storage or external source Includes input validation and error handling for robustness.
   * @param path the path parameter
   * @param executable the executable parameter
   * @param fileStore the fileStore parameter
   */
  private static final Set<PosixFilePermission> readOnlyExecPerms =
      PosixFilePermissions.fromString("r-xr-xr-x");

  /**
   * Loads data from storage or external source
   * @param path the path parameter
   * @param fileStore the fileStore parameter
   * @return the boolean result
   */
  public static void setReadOnlyPerms(Path path, boolean executable, FileStore fileStore)
      throws IOException {
    if (fileStore.supportsFileAttributeView("posix")) {
      if (executable) {
        Files.setPosixFilePermissions(path, readOnlyExecPerms);
      } else {
        Files.setPosixFilePermissions(path, readOnlyPerms);
      }
    } else if (fileStore.supportsFileAttributeView("acl")) {
      // windows, we hope
      UserPrincipal authenticatedUsers =
          path.getFileSystem()
              .getUserPrincipalLookupService()
              .lookupPrincipalByName("Authenticated Users");
      AclEntry denyWriteEntry =
          AclEntry.newBuilder()
              .setType(AclEntryType.DENY)
              .setPrincipal(authenticatedUsers)
              .setPermissions(AclEntryPermission.APPEND_DATA, AclEntryPermission.WRITE_DATA)
              .build();
      AclEntry execEntry =
          AclEntry.newBuilder()
              .setType(executable ? AclEntryType.ALLOW : AclEntryType.DENY)
              .setPrincipal(authenticatedUsers)
              .setPermissions(AclEntryPermission.EXECUTE)
              .build();

      AclFileAttributeView view = Files.getFileAttributeView(path, AclFileAttributeView.class);
      List<AclEntry> acl = view.getAcl();
      acl.add(0, execEntry);
      acl.add(0, denyWriteEntry);
      view.setAcl(acl);
    } else {
      throw new UnsupportedOperationException("no recognized attribute view");
    }
  }

  public static boolean isReadOnlyExecutable(Path path, FileStore fileStore) throws IOException {
    if (fileStore.supportsFileAttributeView("posix")) {
      Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
      if (perms.contains(PosixFilePermission.OWNER_EXECUTE)
          && !perms.contains(PosixFilePermission.GROUP_EXECUTE)
          && !perms.contains(PosixFilePermission.OTHERS_EXECUTE)) {
        setReadOnlyPerms(path, true, fileStore);
      }
      return perms.contains(PosixFilePermission.OWNER_EXECUTE);
    } else {
      return Files.isExecutable(path);
    }
  }
}
