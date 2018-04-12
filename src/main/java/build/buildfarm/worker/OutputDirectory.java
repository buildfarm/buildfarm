// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

// would be great to be able to inherit from ImmutableMap...
public class OutputDirectory extends HashMap<String, OutputDirectory> {
  private OutputDirectory() {
  }

  public static OutputDirectory parse(Iterable<String> outputFiles, Iterable<String> outputDirs) {
    return parseDirectories(Iterables.mergeSorted(
        ImmutableList.<Iterable<String>>of(
            Iterables.transform(
                outputFiles,
                (file) -> file.contains("/") ? file.substring(0, file.lastIndexOf('/') + 1) : ""),
            outputDirs),
        (a, b) -> a.compareTo(b)));
  }

  private static OutputDirectory parseDirectories(Iterable<String> outputDirs) {
    OutputDirectory outputDirectory = new OutputDirectory();
    Stack<OutputDirectory> stack = new Stack<>();

    List<String> sortedOutputDirs = new ArrayList<>();
    Iterables.addAll(sortedOutputDirs, Iterables.transform(outputDirs, (d) -> d + "/"));
    Collections.sort(sortedOutputDirs);

    OutputDirectory currentOutputDirectory = outputDirectory;
    String prefix = "";
    for (String outputDir : sortedOutputDirs) {
      while (!outputDir.startsWith(prefix)) {
        currentOutputDirectory = stack.pop();
        int upPathSeparatorIndex = prefix.lastIndexOf('/', prefix.length() - 2);
        prefix = prefix.substring(0, upPathSeparatorIndex + 1);
      }
      String prefixedFile = outputDir.substring(prefix.length(), outputDir.length() - 1);
      while (prefixedFile.length() > 0) {
        int separatorIndex = prefixedFile.indexOf('/');
        if (separatorIndex == 0) {
          throw new IllegalArgumentException("double separator in output directory");
        }

        String directoryName = separatorIndex == -1 ? prefixedFile : prefixedFile.substring(0, separatorIndex);
        prefix += directoryName + '/';
        prefixedFile = separatorIndex == -1 ? "" : prefixedFile.substring(separatorIndex + 1);
        stack.push(currentOutputDirectory);
        OutputDirectory nextOutputDirectory = new OutputDirectory();
        currentOutputDirectory.put(directoryName, nextOutputDirectory);
        currentOutputDirectory = nextOutputDirectory;
      }
    }

    return outputDirectory;
  }

  public void stamp(Path root) throws IOException {
    if (isEmpty()) {
      Files.createDirectories(root);
    } else {
      for (Map.Entry<String, OutputDirectory> entry : entrySet()) {
        entry.getValue().stamp(root.resolve(entry.getKey()));
      }
    }
  }
}
