// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.worker.util;

import build.bazel.remote.execution.v2.Command;
import com.google.common.collect.ImmutableSet;
import io.prometheus.client.Counter;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * Computes the set of input directory paths excluded from directory-level linking. These paths must
 * remain real directories because they are ancestors of output paths or tool input paths. A
 * directory NOT in the returned set can be replaced with a single link to its CAS materialized
 * tree. A directory IN the set must be created as a real directory and descended into.
 *
 * <p>Shared between CFCLinkExecFileSystem (for exec dir creation) and ProtoCoordinator (for
 * persistent worker input linking).
 */
public final class LinkedInputExclusions {
  // Diagnostic: why directories get excluded from directory-level linking. Counted once per source
  // declaration (not per expanded ancestor) at computeExclusionSet time. A new or ballooning reason
  // after a config/REAPI change is a tripwire that linking coverage is shrinking.
  private static final Counter linkedInputExclusionsTotal =
      Counter.build()
          .name("linked_input_exclusions_total")
          .labelNames("reason")
          .help("Directory-linking exclusions by source (output_path / output_file / "
              + "output_directory / tool_input / exclude_directory).")
          .register();

  private LinkedInputExclusions() {}

  public static final class ExclusionSet {
    private static final ExclusionSet EMPTY =
        new ExclusionSet(ImmutableSet.of(), ImmutableSet.of());

    private final ImmutableSet<String> paths;
    private final ImmutableSet<String> recursivePaths;

    private ExclusionSet(ImmutableSet<String> paths, ImmutableSet<String> recursivePaths) {
      this.paths = paths;
      this.recursivePaths = recursivePaths;
    }

    public static ExclusionSet empty() {
      return EMPTY;
    }

    public ImmutableSet<String> paths() {
      return paths;
    }

    public ImmutableSet<String> recursivePaths() {
      return recursivePaths;
    }

    public boolean excludes(String path) {
      if (paths.contains(path)) {
        return true;
      }
      if (recursivePaths.contains("")) {
        return true;
      }

      int slashIndex = path.length();
      while ((slashIndex = path.lastIndexOf('/', slashIndex - 1)) != -1) {
        if (recursivePaths.contains(path.substring(0, slashIndex))) {
          return true;
        }
      }
      return false;
    }
  }

  private static String stripTrailingSlash(String path) {
    while (path.length() > 1 && path.charAt(path.length() - 1) == '/') {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Normalizes a path string by collapsing {@code .} and {@code ..} segments. Rejects absolute
   * paths since all exclusion paths must be relative to the input root.
   */
  private static String normalizePath(String path) {
    if (!path.isEmpty() && path.charAt(0) == '/') {
      throw new IllegalArgumentException(
          "LinkedInputExclusions requires relative paths, got absolute: " + path);
    }
    if (path.indexOf('\0') != -1) {
      throw new IllegalArgumentException("LinkedInputExclusions path contains NUL: " + path);
    }
    // Most paths from the REAPI Command are already normalized. Keep this normalization independent
    // of the host filesystem so these sets always contain REAPI-style slash-separated paths.
    if (!needsNormalization(path)) {
      return path;
    }

    ArrayDeque<String> segments = new ArrayDeque<>();
    for (String segment : path.split("/", -1)) {
      if (segment.isEmpty() || segment.equals(".")) {
        continue;
      }
      if (segment.equals("..")) {
        if (!segments.isEmpty() && !segments.peekLast().equals("..")) {
          segments.removeLast();
        } else {
          segments.addLast(segment);
        }
      } else {
        segments.addLast(segment);
      }
    }
    return String.join("/", segments);
  }

  private static boolean needsNormalization(String path) {
    int length = path.length();
    for (int i = 0; i < length; i++) {
      char c = path.charAt(i);
      if (c == '/') {
        if (i + 1 == length || path.charAt(i + 1) == '/') {
          return true;
        }
      } else if (c == '.') {
        boolean atStart = (i == 0) || path.charAt(i - 1) == '/';
        if (!atStart) {
          continue;
        }
        if (i + 1 == length || path.charAt(i + 1) == '/') {
          return true;
        }
        if (path.charAt(i + 1) == '.' && (i + 2 == length || path.charAt(i + 2) == '/')) {
          return true;
        }
      }
    }
    return false;
  }

  /** Adds all proper ancestor directory paths of the given path. The path itself is NOT added. */
  private static void addAncestors(String path, Set<String> ancestors) {
    int slashIndex = path.indexOf('/');
    while (slashIndex != -1) {
      ancestors.add(path.substring(0, slashIndex));
      slashIndex = path.indexOf('/', slashIndex + 1);
    }
  }

  private static void addPathAndAncestors(String path, Set<String> paths) {
    paths.add(path);
    addAncestors(path, paths);
  }

  private static String resolveAgainstWorkingDirectory(String workingDirectory, String path) {
    return normalizePath(
        stripTrailingSlash(workingDirectory.isEmpty() ? path : workingDirectory + "/" + path));
  }

  /**
   * Returns {@code path} relative to {@code root}, using REAPI's '/' separator on every host OS.
   */
  public static String pathToRelativeString(Path root, Path path) {
    Path relative = root.relativize(path);
    if (relative.toString().isEmpty()) {
      return "";
    }

    StringBuilder result = new StringBuilder();
    for (Path segment : relative) {
      if (result.length() != 0) {
        result.append('/');
      }
      result.append(segment);
    }
    return result.toString();
  }

  /**
   * Computes the input directory paths excluded from directory-level linking.
   *
   * @param command the action command with output path declarations
   * @param excludePaths additional paths whose ancestors must be real (e.g., tool inputs for
   *     persistent workers). These are relative paths within the input tree.
   * @param excludeDirectories additional directory paths that must be real.
   * @return exact and recursive relative path strings that must remain real directories
   */
  public static ExclusionSet computeExclusionSet(
      Command command, Set<String> excludePaths, Set<String> excludeDirectories) {
    Set<String> exclusions = new HashSet<>();
    Set<String> recursiveExclusions = new HashSet<>();
    String workingDirectory = stripTrailingSlash(command.getWorkingDirectory());

    if (command.getOutputPathsCount() != 0) {
      // REAPI >= 2.1: output_paths entries can be files OR directories — the type is not
      // known here, so conservatively treat each path as a potential directory that must
      // remain real and writable.
      for (String outputPath : command.getOutputPathsList()) {
        String resolved = resolveAgainstWorkingDirectory(workingDirectory, outputPath);
        addPathAndAncestors(resolved, exclusions);
        recursiveExclusions.add(resolved);
        linkedInputExclusionsTotal.labels("output_path").inc();
      }
    } else {
      // REAPI < 2.1: for output_files only the parent directory + ancestors must remain
      // real (the entry itself is a file, not a directory).
      for (String outputFile : command.getOutputFilesList()) {
        String resolved = resolveAgainstWorkingDirectory(workingDirectory, outputFile);
        int lastSlash = resolved.lastIndexOf('/');
        if (lastSlash != -1) {
          String parent = resolved.substring(0, lastSlash);
          exclusions.add(parent);
          addAncestors(parent, exclusions);
          linkedInputExclusionsTotal.labels("output_file").inc();
        }
      }
      for (String outputDir : command.getOutputDirectoriesList()) {
        String resolved = resolveAgainstWorkingDirectory(workingDirectory, outputDir);
        addPathAndAncestors(resolved, exclusions);
        recursiveExclusions.add(resolved);
        linkedInputExclusionsTotal.labels("output_directory").inc();
      }
    }

    // Exclude paths are file paths (e.g., tool input files). Only their ancestor
    // directories must remain real — the file path itself is never checked during
    // directory traversal.
    for (String excludePath : excludePaths) {
      String normalized = normalizePath(stripTrailingSlash(excludePath));
      addAncestors(normalized, exclusions);
      linkedInputExclusionsTotal.labels("tool_input").inc();
    }

    // Exclude directories are directory paths. Their ancestors must also remain real so traversal
    // can reach the matching directory instead of symlinking an ancestor first.
    for (String excludeDirectory : excludeDirectories) {
      String normalized = normalizePath(stripTrailingSlash(excludeDirectory));
      addPathAndAncestors(normalized, exclusions);
      linkedInputExclusionsTotal.labels("exclude_directory").inc();
    }

    return new ExclusionSet(
        ImmutableSet.copyOf(exclusions), ImmutableSet.copyOf(recursiveExclusions));
  }

  public static ExclusionSet computeExclusionSet(Command command, Set<String> excludePaths) {
    return computeExclusionSet(command, excludePaths, ImmutableSet.of());
  }

  /**
   * Computes the exact input directory paths excluded from directory-level linking.
   *
   * <p>Use {@link #computeExclusionSet(Command, Set)} for traversal decisions that must also honor
   * recursive output directory roots.
   */
  public static ImmutableSet<String> compute(Command command, Set<String> excludePaths) {
    return computeExclusionSet(command, excludePaths).paths();
  }
}
