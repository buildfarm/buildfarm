// Copyright 2022-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common.config;

import com.google.common.base.Strings;
import java.nio.file.Path;
import javax.naming.ConfigurationException;
import lombok.Data;

@Data
public class Cas {
  public enum TYPE {
    FILESYSTEM,
    GRPC,
    MEMORY,
    FUSE
  }

  private TYPE type = TYPE.FILESYSTEM;

  // MEMORY/FILESYSTEM
  private String path = "cache";
  private int hexBucketLevels = 0;
  private long maxSizeBytes = 0;
  private Integer maxSizePercent;
  private boolean fileDirectoriesIndexInMemory = false;
  private boolean skipLoad = false;

  // if creating a hardlink fails, copy the file instead
  private boolean execRootCopyFallback = false;

  // GRPC
  private String target;
  private boolean readonly = false;

  public Path getValidPath(Path root) throws ConfigurationException {
    if (Strings.isNullOrEmpty(path)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(path);
  }
}
