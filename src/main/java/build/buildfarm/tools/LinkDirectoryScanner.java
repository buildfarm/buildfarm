// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.tools;

import static build.buildfarm.common.grpc.Channels.createChannel;
import static build.buildfarm.instance.Utils.getBlob;
import static build.buildfarm.server.services.OperationsService.LIST_OPERATIONS_MAXIMUM_PAGE_SIZE;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.stub.StubInstance;
import build.buildfarm.v1test.Digest;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.QueuedOperation;
import build.buildfarm.v1test.QueuedOperationMetadata;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.OutputDirectory;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import picocli.CommandLine;

/**
 * Scans a random sample of Operations from the Buildfarm backplane and counts how many times each
 * linkable input directory appears across the sample.
 *
 * <p>A directory is considered linkable if it contains no output files declared in the Command
 * (i.e., its node in the OutputDirectory tree is null). Results are sorted by count to help
 * identify which directory patterns would yield the most cache reuse if targeted by the {@code
 * linkedInputDirectories} configuration.
 *
 * <p>Operations are enumerated through the Buildfarm Operations API. The inline Action available in
 * {@code QueuedOperationMetadata} is used to avoid a separate gRPC blob fetch for the Action. A
 * single {@code QueuedOperation} blob fetch provides the Command and the complete input tree,
 * replacing the previous approach of 3+ separate gRPC calls per operation.
 */
@picocli.CommandLine.Command(
    name = "bf-link-scanner",
    mixinStandardHelpOptions = true,
    description = {
      "Sample Operations from the backplane and report how many times each linkable",
      "input directory (one containing no output files) appears across the sample.",
      "Sort results by appearances×fileCount to identify which regex patterns for",
      "linkedInputDirectories would yield the greatest cache reuse.",
      "Operations whose retained CAS blobs have expired are skipped and summarized."
    })
class LinkDirectoryScanner implements Callable<Integer> {
  @picocli.CommandLine.Parameters(
      index = "0",
      description =
          "Buildfarm server gRPC endpoint (host:port) — used to fetch QueuedOperation blobs")
  private String host;

  @picocli.CommandLine.Option(
      names = {"-i", "--instance"},
      description = "Instance name",
      defaultValue = "shard")
  private String instanceName;

  @picocli.CommandLine.Option(
      names = {"-n", "--sample"},
      description = "Number of operations to include in the random sample (reservoir sampling)",
      defaultValue = "1000")
  private int sampleSize;

  @picocli.CommandLine.Option(
      names = {"--max-scan"},
      description =
          "Maximum number of operations to visit during the scan before stopping."
              + " The sample is drawn uniformly from the visited set."
              + " -1 means scan all operations (potentially slow for a large backplane).",
      defaultValue = "-1")
  private int maxScan;

  @picocli.CommandLine.Option(
      names = {"--top"},
      description = "Number of top directories to print in the results table",
      defaultValue = "50")
  private int topN;

  @picocli.CommandLine.Option(
      names = {"--bottom"},
      description =
          "Number of bottom directories to print in the results table. These are the ones you"
              + " probably want to be sure you aren't linking since they have low reuse but still"
              + " consume cache space.",
      defaultValue = "0")
  private int bottomN;

  @picocli.CommandLine.Option(
      names = {"--link"},
      description =
          "Regex pattern treated as a linkedInputDirectories entry."
              + " Matched directories are tallied individually but their subdirectories are not."
              + " May be repeated.",
      arity = "1")
  private List<String> linkPatterns = new ArrayList<>();

  @picocli.CommandLine.Option(
      names = {"--filter"},
      description = "Only display result rows whose path matches this regex.")
  private String filterPattern;

  /** Recursive size and file count for one directory (memoised across all operations). */
  private record DirStats(long sizeBytes, long fileCount) {
    static final DirStats EMPTY = new DirStats(0, 0);
  }

  /** Outcome of reading the blobs needed to analyze one listed operation. */
  private enum ProcessResult {
    PROCESSED,
    MISSING_QUEUED_OPERATION("queued-operation blob was unavailable"),
    MISSING_ACTION("action blob was unavailable"),
    MISSING_COMMAND("command blob was unavailable"),
    MISSING_INPUT_ROOT("input root was unavailable or absent"),
    UNSUPPORTED_METADATA("operation metadata could not be decoded");

    private final String description;

    ProcessResult() {
      this.description = null;
    }

    ProcessResult(String description) {
      this.description = description;
    }
  }

  /** Displays processing progress without requiring a third-party terminal UI dependency. */
  private static final class ProgressReporter {
    private final int total;
    private final boolean interactive = System.console() != null;

    ProgressReporter(int total) {
      this.total = total;
    }

    void update(int complete, int processed, int skipped, int errors) {
      if (interactive) {
        int width = 30;
        int filled = total == 0 ? width : width * complete / total;
        String bar = "#".repeat(filled) + "-".repeat(width - filled);
        System.err.printf(
            "\rProcessing [%s] %3d%%  %,d/%,d  processed: %,d  skipped: %,d  errors: %,d",
            bar,
            total == 0 ? 100 : complete * 100 / total,
            complete,
            total,
            processed,
            skipped,
            errors);
      } else if (complete == 1 || complete == total || complete % 10 == 0) {
        System.err.printf(
            "  Processing: %,d/%,d operations  processed: %,d  skipped: %,d  errors: %,d%n",
            complete, total, processed, skipped, errors);
      }
    }

    void finish() {
      if (interactive) {
        System.err.println();
      }
    }
  }

  /**
   * One entry in a fully-linkable subtree: a descendant directory, expressed as a path relative to
   * the subtree root, with its cache key and pre-computed stats.
   */
  private record SubtreeEntry(String relPath, String cacheKey, long sizeBytes, long fileCount) {}

  /**
   * Aggregated data for one directory cache key: total appearance count, size/file-count (same for
   * every appearance since the key is content-addressed), and per-path appearance counts.
   */
  private static final class KeyData {
    long totalCount;
    long sizeBytes;
    long fileCount;
    final Map<String, Long> pathCounts = new HashMap<>();

    void record(String path, long sizeBytes, long fileCount) {
      totalCount++;
      this.sizeBytes = sizeBytes;
      this.fileCount = fileCount;
      pathCounts.merge(path, 1L, Long::sum);
    }
  }

  /** Aggregated stats for all directories that appeared at a given input-tree path. */
  private static final class PathStats {
    int uniqueDirs;
    long totalAppearances;
    // Weighted sums for computing averages (weight = appearances of that specific directory).
    long weightedFileSum;
    long weightedSizeSum;
    // Unweighted: each distinct directory's size/fileCount counted once (the cache footprint).
    long uniqueSizeSum;
    long uniqueFileSum;

    void add(long appearances, long fileCount, long sizeBytes) {
      uniqueDirs++;
      totalAppearances += appearances;
      weightedFileSum += fileCount * appearances;
      weightedSizeSum += sizeBytes * appearances;
      uniqueSizeSum += sizeBytes;
      uniqueFileSum += fileCount;
    }

    double avgFiles() {
      return totalAppearances == 0 ? 0 : (double) weightedFileSum / totalAppearances;
    }

    double avgSizeBytes() {
      return totalAppearances == 0 ? 0 : (double) weightedSizeSum / totalAppearances;
    }

    /**
     * Number of file links that would be saved by linking this directory, across all appearances in
     * the sample.
     *
     * <ul>
     *   <li>weightedFileSum: Total number of directories/files that would be created if never
     *       linked
     *   <li>uniqueFileSum: Total number of directories/files that would be created in the CAS
     *       `_dir`s when linked
     *   <li>totalAppearances: Still have to create one symlink to the CAS dir for each appearance
     * </ul>
     */
    double savedLinks() {
      return weightedFileSum - uniqueFileSum - totalAppearances;
    }

    double totalSizeMb() {
      return uniqueSizeSum / 1024.0 / 1024.0;
    }

    /**
     * Cache-hit file links divided by total unique cache footprint. The first appearance of each
     * unique directory is excluded from the numerator since it populates the cache rather than
     * hitting it. Higher = more file links saved per byte stored on disk.
     */
    double score() {
      if (uniqueSizeSum == 0) return 0;
      return savedLinks() / totalSizeMb();
    }
  }

  /** Holds the cross-operation directory caches with hit/miss tracking. */
  private static final class Caches {
    final Map<String, Directory> dirCache = new HashMap<>();
    final Map<String, DirStats> statsCache = new HashMap<>();
    final Map<String, List<SubtreeEntry>> subtreeMemo = new HashMap<>();

    long statsCacheHits;
    long statsCacheMisses;
    long subtreeMemoHits;
    long subtreeMemoMisses;
    long dirCacheHits;
    long dirCacheMisses;

    DirStats getStats(String hash) {
      DirStats s = statsCache.get(hash);
      if (s != null) statsCacheHits++;
      else statsCacheMisses++;
      return s;
    }

    List<SubtreeEntry> getSubtree(String hash) {
      List<SubtreeEntry> s = subtreeMemo.get(hash);
      if (s != null) subtreeMemoHits++;
      else subtreeMemoMisses++;
      return s;
    }

    Directory getDir(String hash) {
      Directory d = dirCache.get(hash);
      if (d != null) dirCacheHits++;
      else dirCacheMisses++;
      return d;
    }

    void printStats() {
      System.err.println("Cache statistics:");
      printRow("globalStatsCache", statsCache.size(), statsCacheHits, statsCacheMisses);
      printRow("subtreeMemo", subtreeMemo.size(), subtreeMemoHits, subtreeMemoMisses);
      printRow("globalDirCache", dirCache.size(), dirCacheHits, dirCacheMisses);
    }

    private static void printRow(String name, int size, long hits, long misses) {
      long total = hits + misses;
      String hitRate = total == 0 ? "n/a" : String.format("%.0f%%", 100.0 * hits / total);
      System.err.printf(
          "  %-22s  size: %,7d  hits: %,8d  misses: %,8d  hit rate: %s%n",
          name, size, hits, misses, hitRate);
    }
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new LinkDirectoryScanner()).execute(args));
  }

  @Override
  public Integer call() throws Exception {
    System.err.printf("Connecting to gRPC server at %s (instance=%s)...%n", host, instanceName);
    System.err.flush();
    ManagedChannel channel = createChannel(host);
    Instance instance =
        new StubInstance(instanceName, "bf-link-scanner", channel, Durations.fromSeconds(60));
    System.err.println("gRPC channel created (lazy — will connect on first use)");
    System.err.flush();
    try {
      List<Operation> sample = collectSample(instance);
      System.err.printf("Sampled %d operations from backplane%n", sample.size());

      Map<String, KeyData> data = new HashMap<>();
      Caches caches = new Caches();
      List<Pattern> linkRegexes =
          linkPatterns.stream().map(Pattern::compile).collect(Collectors.toList());

      int processed = 0;
      int errors = 0;
      int total = sample.size();
      Map<ProcessResult, Integer> skipped = new EnumMap<>(ProcessResult.class);
      ProgressReporter progress = new ProgressReporter(total);
      System.err.printf(
          "Processing %,d sampled operations (fetching queued-operation, action, and input-tree"
              + " blobs)...%n",
          total);
      for (Operation operation : sample) {
        try {
          ProcessResult result = processOperation(instance, operation, caches, linkRegexes, data);
          if (result == ProcessResult.PROCESSED) {
            processed++;
          } else {
            skipped.merge(result, 1, Integer::sum);
          }
        } catch (Exception e) {
          errors++;
          if (errors <= 10) {
            System.err.printf(
                "Warning: skipped operation %s: %s%n", operation.getName(), e.getMessage());
          }
        }
        int done = processed + errors;
        done += skipped.values().stream().mapToInt(Integer::intValue).sum();
        progress.update(done, processed, done - processed - errors, errors);
      }
      progress.finish();
      int skippedCount = skipped.values().stream().mapToInt(Integer::intValue).sum();
      System.err.printf(
          "Processed: %d, Skipped: %d, Failed: %d%n", processed, skippedCount, errors);
      skipped.forEach(
          (reason, count) -> System.err.printf("  Skipped %,d: %s%n", count, reason.description));
      printResults(data);
      System.out.flush();
      caches.printStats();
    } finally {
      instance.stop();
    }
    return 0;
  }

  /**
   * Pages through the Buildfarm Operations API and builds a reservoir sample of size {@link
   * #sampleSize}, stopping after {@link #maxScan} operations when set.
   */
  private List<Operation> collectSample(Instance instance) throws IOException {
    int[] totalOperations = {0};
    List<Operation> reservoir = new ArrayList<>(sampleSize);
    Random random = new Random();
    String pageToken = Instance.SENTINEL_PAGE_TOKEN;
    int pagesScanned = 0;
    System.err.printf(
        "Listing operations in pages of %d%s...%n",
        LIST_OPERATIONS_MAXIMUM_PAGE_SIZE, maxScan > 0 ? " (stopping after " + maxScan + ")" : "");
    do {
      pageToken =
          instance.listOperations(
              "executions",
              LIST_OPERATIONS_MAXIMUM_PAGE_SIZE,
              pageToken,
              "",
              operation -> {
                if (maxScan > 0 && totalOperations[0] >= maxScan) {
                  return;
                }
                totalOperations[0]++;
                if (reservoir.size() < sampleSize) {
                  reservoir.add(operation);
                } else {
                  int j = random.nextInt(totalOperations[0]);
                  if (j < sampleSize) {
                    reservoir.set(j, operation);
                  }
                }
              });
      pagesScanned++;
      System.err.printf(
          "  Listed page %,d: %,d operations visited, %,d sampled%n",
          pagesScanned, totalOperations[0], reservoir.size());
      if (maxScan > 0 && totalOperations[0] >= maxScan) {
        System.err.printf("Scanned %d operations (--max-scan limit reached)%n", totalOperations[0]);
        break;
      }
    } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
    System.err.printf("Scanned %d operations%n", totalOperations[0]);
    return reservoir;
  }

  /**
   * Processes one operation: fetches (or reuses cached) Command and input tree, then walks the tree
   * to record every linkable directory.
   *
   * <p><b>Fast path (QueuedOperationMetadata):</b> The inline Action is extracted directly from the
   * metadata returned by the Operations API, and a single {@code QueuedOperation} blob fetch
   * provides both the Command and the complete input tree.
   *
   * <p><b>Fallback path (other metadata types):</b> Falls back to separate gRPC fetches for the
   * Action blob, Command blob, and tree pages. Used for completed operations that no longer carry
   * QueuedOperationMetadata.
   */
  private static ProcessResult processOperation(
      Instance instance,
      Operation operation,
      Caches caches,
      List<Pattern> linkRegexes,
      Map<String, KeyData> data)
      throws IOException, InterruptedException {
    DigestFunction.Value digestFunction;
    String inputRootHash;
    Command command;

    if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
      // Fetch the QueuedOperation blob — it contains Action, Command, and the full input tree.
      QueuedOperationMetadata queuedMeta =
          operation.getMetadata().unpack(QueuedOperationMetadata.class);
      digestFunction = queuedMeta.getExecuteOperationMetadata().getDigestFunction();
      ByteString blob =
          getBlob(
              instance,
              Compressor.Value.IDENTITY,
              queuedMeta.getQueuedOperationDigest(),
              RequestMetadata.getDefaultInstance());
      if (blob == null) {
        return ProcessResult.MISSING_QUEUED_OPERATION;
      }
      QueuedOperation queuedOp = QueuedOperation.parseFrom(blob);
      Action action = queuedOp.getAction();
      if (action.getInputRootDigest().getHash().isEmpty()) {
        return ProcessResult.MISSING_INPUT_ROOT;
      }
      inputRootHash = DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction).getHash();
      command = queuedOp.getCommand();
      queuedOp.getTree().getDirectoriesMap().forEach(caches.dirCache::putIfAbsent);
    } else {
      // Fallback path for completed operations that no longer carry QueuedOperationMetadata.
      ExecuteOperationMetadata metadata = extractMetadata(operation);
      if (metadata == null) {
        return ProcessResult.UNSUPPORTED_METADATA;
      }
      digestFunction = metadata.getDigestFunction();
      Digest actionDigest = DigestUtil.fromDigest(metadata.getActionDigest(), digestFunction);

      ByteString actionBlob =
          getBlob(
              instance,
              Compressor.Value.IDENTITY,
              actionDigest,
              RequestMetadata.getDefaultInstance());
      if (actionBlob == null) {
        return ProcessResult.MISSING_ACTION;
      }
      Action action = Action.parseFrom(actionBlob);

      Digest inputRootDigest = DigestUtil.fromDigest(action.getInputRootDigest(), digestFunction);
      inputRootHash = inputRootDigest.getHash();

      ByteString commandBlob =
          getBlob(
              instance,
              Compressor.Value.IDENTITY,
              DigestUtil.fromDigest(action.getCommandDigest(), digestFunction),
              RequestMetadata.getDefaultInstance());
      if (commandBlob == null) {
        return ProcessResult.MISSING_COMMAND;
      }
      command = Command.parseFrom(commandBlob);

      Tree.Builder treeBuilder = Tree.newBuilder();
      String pageToken = Instance.SENTINEL_PAGE_TOKEN;
      do {
        pageToken = instance.getTree(inputRootDigest, 1024, pageToken, treeBuilder);
      } while (!pageToken.equals(Instance.SENTINEL_PAGE_TOKEN));
      treeBuilder.build().getDirectoriesMap().forEach(caches.dirCache::putIfAbsent);
    }

    // Build the OutputDirectory tree from declared outputs.
    OutputDirectory outputDirRoot =
        OutputDirectory.parse(
            command.getOutputFilesList(),
            command.getOutputDirectoriesList(),
            command.getEnvironmentVariablesList());

    Directory rootDir = caches.getDir(inputRootHash);
    if (rootDir == null) {
      return ProcessResult.MISSING_INPUT_ROOT;
    }
    traverseDir(rootDir, caches, outputDirRoot, digestFunction, "", linkRegexes, data);
    return ProcessResult.PROCESSED;
  }

  /**
   * DFS traversal of the input directory tree.
   *
   * <p>For every child directory whose {@code OutputDirectory} node is null (no declared outputs),
   * its cache key, path, and stats are recorded and all of its descendants are handled via {@code
   * subtreeMemo} — avoiding a second tree walk when the same hash appears in a later operation.
   * When the child does have declared outputs, traversal continues normally so that linkable
   * subdirectories nested inside output-containing parents are still counted.
   *
   * @param outputDir the {@code OutputDirectory} node for {@code dir}; null means the entire
   *     subtree has no outputs and every descendant directory is linkable
   * @param parentPath slash-separated path of {@code dir} relative to the input root (empty for the
   *     root itself)
   * @param globalStatsCache cross-operation cache for {@link #computeStats}
   * @param subtreeMemo cross-operation cache: directory hash → flat list of all descendant
   *     directories for fully-linkable subtrees
   */
  private static void traverseDir(
      Directory dir,
      Caches caches,
      OutputDirectory outputDir,
      DigestFunction.Value digestFunction,
      String parentPath,
      List<Pattern> linkRegexes,
      Map<String, KeyData> data) {
    for (DirectoryNode child : dir.getDirectoriesList()) {
      String childPath =
          parentPath.isEmpty() ? child.getName() : parentPath + "/" + child.getName();
      String childHash = child.getDigest().getHash();
      OutputDirectory childOutputDir =
          (outputDir != null) ? outputDir.getChild(child.getName()) : null;

      if (childOutputDir == null) {
        // No declared outputs under this directory — it is a candidate for linking.
        DirStats stats = computeStats(childHash, caches);
        data.computeIfAbsent(cacheKey(child.getDigest(), digestFunction), k -> new KeyData())
            .record(childPath, stats.sizeBytes(), stats.fileCount());

        if (matchesAnyLink(childPath, linkRegexes)) {
          // This directory is claimed by a --link regex. Its descendants are covered by the
          // link and should not be tallied individually.
          continue;
        }

        // All descendants are also fully linkable. Record them, but stop descending into any
        // descendant that itself matches a --link regex (it will be tallied at its own path).
        // getOrComputeDescendants returns entries in DFS order (parent before children), so
        // tracking a single active linked prefix is sufficient to suppress a whole subtree.
        List<SubtreeEntry> descendants = getOrComputeDescendants(childHash, caches, digestFunction);
        String linkedRelPrefix = null;
        for (SubtreeEntry e : descendants) {
          if (linkedRelPrefix != null && e.relPath().startsWith(linkedRelPrefix + "/")) {
            continue; // under a linked ancestor, suppressed
          }
          linkedRelPrefix = null;
          String descPath = childPath + "/" + e.relPath();
          data.computeIfAbsent(e.cacheKey(), k -> new KeyData())
              .record(descPath, e.sizeBytes(), e.fileCount());
          if (matchesAnyLink(descPath, linkRegexes)) {
            linkedRelPrefix = e.relPath(); // suppress this subtree's descendants
          }
        }
      } else {
        // This child contains declared outputs; recurse normally to find linkable sub-dirs.
        Directory childDir = caches.getDir(childHash);
        if (childDir != null) {
          traverseDir(
              childDir, caches, childOutputDir, digestFunction, childPath, linkRegexes, data);
        }
      }
    }
  }

  private static boolean matchesAnyLink(String path, List<Pattern> linkRegexes) {
    for (Pattern p : linkRegexes) {
      if (p.matcher(path).matches()) return true;
    }
    return false;
  }

  /**
   * Returns the flat list of all descendant directories inside the subtree rooted at {@code hash},
   * with paths relative to that root. Populates {@code subtreeMemo} for every subdirectory visited,
   * so that any of them can be reused as memo hits in future operations without further traversal.
   */
  private static List<SubtreeEntry> getOrComputeDescendants(
      String hash, Caches caches, DigestFunction.Value digestFunction) {
    List<SubtreeEntry> cached = caches.getSubtree(hash);
    if (cached != null) {
      return cached;
    }
    Directory dir = caches.getDir(hash);
    if (dir == null) {
      caches.subtreeMemo.put(hash, List.of());
      return List.of();
    }
    List<SubtreeEntry> result = new ArrayList<>();
    for (DirectoryNode child : dir.getDirectoriesList()) {
      String childHash = child.getDigest().getHash();
      DirStats stats = computeStats(childHash, caches);
      String childKey = cacheKey(child.getDigest(), digestFunction);
      result.add(new SubtreeEntry(child.getName(), childKey, stats.sizeBytes(), stats.fileCount()));
      // Recursively get (and memoize) the child's own descendants, then add them with a prefix.
      for (SubtreeEntry e : getOrComputeDescendants(childHash, caches, digestFunction)) {
        result.add(
            new SubtreeEntry(
                child.getName() + "/" + e.relPath(), e.cacheKey(), e.sizeBytes(), e.fileCount()));
      }
    }
    caches.subtreeMemo.put(hash, result);
    return result;
  }

  /**
   * Recursively computes total size (sum of all file sizes) and total file count for the directory
   * identified by {@code hash}. Results are memoised in {@code globalStatsCache} which is shared
   * across all operations — safe because directories are content-addressed.
   */
  private static DirStats computeStats(String hash, Caches caches) {
    DirStats cached = caches.getStats(hash);
    if (cached != null) {
      return cached;
    }
    Directory dir = caches.getDir(hash);
    if (dir == null) {
      caches.statsCache.put(hash, DirStats.EMPTY);
      return DirStats.EMPTY;
    }
    // Count files and subdirectory nodes — both represent entries that must be linked/created.
    long fileCount = dir.getFilesCount() + dir.getDirectoriesCount();
    // Each directory occupies at least one 4 KB filesystem block.
    long size = 4096;
    for (FileNode file : dir.getFilesList()) {
      // Round each file up to the nearest 4 KB block.
      long fileSize = file.getDigest().getSizeBytes();
      size += (fileSize + 4095) / 4096 * 4096;
    }
    for (DirectoryNode child : dir.getDirectoriesList()) {
      DirStats childStats = computeStats(child.getDigest().getHash(), caches);
      size += childStats.sizeBytes();
      fileCount += childStats.fileCount();
    }
    DirStats stats = new DirStats(size, fileCount);
    caches.statsCache.put(hash, stats);
    return stats;
  }

  /**
   * Computes the CAS directory cache key for a given directory digest.
   *
   * <p>Mirrors {@code CASFileCache.getDirectoryKey}: for digest functions in {@link
   * DigestUtil.OMITTED_DIGEST_FUNCTIONS} the key is {@code hash + "_dir"}; for others it is {@code
   * digestFunction_lower + "_" + hash + "_dir"}.
   */
  private static String cacheKey(
      build.bazel.remote.execution.v2.Digest digest, DigestFunction.Value digestFunction) {
    String prefix =
        DigestUtil.OMITTED_DIGEST_FUNCTIONS.contains(digestFunction)
            ? ""
            : digestFunction.toString().toLowerCase() + "_";
    return prefix + digest.getHash() + "_dir";
  }

  /**
   * Extracts {@link ExecuteOperationMetadata} from whichever metadata type the operation carries.
   * Returns null if the metadata cannot be parsed.
   */
  private static ExecuteOperationMetadata extractMetadata(Operation operation) {
    try {
      if (operation.getMetadata().is(QueuedOperationMetadata.class)) {
        return operation
            .getMetadata()
            .unpack(QueuedOperationMetadata.class)
            .getExecuteOperationMetadata();
      } else if (operation.getMetadata().is(ExecuteEntry.class)) {
        return toExecuteOperationMetadata(operation.getMetadata().unpack(ExecuteEntry.class));
      } else if (operation.getMetadata().is(QueueEntry.class)) {
        return toExecuteOperationMetadata(
            operation.getMetadata().unpack(QueueEntry.class).getExecuteEntry());
      } else if (operation.getMetadata().is(DispatchedOperation.class)) {
        return toExecuteOperationMetadata(
            operation
                .getMetadata()
                .unpack(DispatchedOperation.class)
                .getQueueEntry()
                .getExecuteEntry());
      } else {
        return operation.getMetadata().unpack(ExecuteOperationMetadata.class);
      }
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private static ExecuteOperationMetadata toExecuteOperationMetadata(ExecuteEntry entry) {
    return ExecuteOperationMetadata.newBuilder()
        .setActionDigest(DigestUtil.toDigest(entry.getActionDigest()))
        .setDigestFunction(entry.getActionDigest().getDigestFunction())
        .build();
  }

  private void printResults(Map<String, KeyData> data) {
    // Invert from (cacheKey → pathCounts) to (path → PathStats).
    Map<String, PathStats> pathData = new HashMap<>();
    for (KeyData kd : data.values()) {
      for (Map.Entry<String, Long> pe : kd.pathCounts.entrySet()) {
        pathData
            .computeIfAbsent(pe.getKey(), k -> new PathStats())
            .add(pe.getValue(), kd.fileCount, kd.sizeBytes);
      }
    }

    // Sort by score descending, then apply optional display filter.
    List<Map.Entry<String, PathStats>> sorted = new ArrayList<>(pathData.entrySet());
    sorted.sort(
        Comparator.<Map.Entry<String, PathStats>>comparingDouble(e -> e.getValue().score())
            .reversed());
    if (filterPattern != null) {
      Pattern filter = Pattern.compile(filterPattern);
      sorted.removeIf(e -> !filter.matcher(e.getKey()).matches());
    }

    List<Pattern> linkRegexes =
        linkPatterns.stream().map(Pattern::compile).collect(Collectors.toList());

    if (!linkPatterns.isEmpty()) {
      double[] savedLinks = new double[linkPatterns.size()];
      double[] sizeSums = new double[linkPatterns.size()];
      long[] totalAppearances = new long[linkPatterns.size()];
      int[] pathCounts = new int[linkPatterns.size()];
      for (Map.Entry<String, PathStats> entry : pathData.entrySet()) {
        PathStats ps = entry.getValue();
        for (int i = 0; i < linkRegexes.size(); i++) {
          if (linkRegexes.get(i).matcher(entry.getKey()).matches()) {
            savedLinks[i] += ps.savedLinks();
            sizeSums[i] += ps.totalSizeMb();
            totalAppearances[i] += ps.totalAppearances;
            pathCounts[i]++;
          }
        }
      }
      System.out.println("Score\tAppearances\tPaths Matched\tRegex");
      for (int i = 0; i < linkPatterns.size(); i++) {
        System.out.printf(
            "%.0f\t%d\t%d\t%s%n",
            pathCounts[i] * savedLinks[i] / sizeSums[i],
            totalAppearances[i],
            pathCounts[i],
            linkPatterns.get(i));
      }
      System.out.println();
    }

    System.out.println("Linked\tScore\tAppearances\tUnique\tAvg Files\tAvg Size\tPath");
    int topLimit = Math.min(topN, sorted.size());
    for (int i = 0; i < topLimit; i++) {
      printRow(sorted.get(i), linkRegexes);
    }

    if (bottomN > 0 && sorted.size() > topLimit) {
      int bottomStart = Math.max(topLimit, sorted.size() - bottomN);
      if (bottomStart > topLimit) {
        System.out.println("...");
      }
      for (int i = bottomStart; i < sorted.size(); i++) {
        printRow(sorted.get(i), linkRegexes);
      }
    }
  }

  private static void printRow(Map.Entry<String, PathStats> entry, List<Pattern> linkRegexes) {
    PathStats ps = entry.getValue();
    String linked = matchesAnyLink(entry.getKey(), linkRegexes) ? "*" : "";
    System.out.printf(
        "%s\t%.0f\t%d\t%d\t%.0f\t%s\t%s%n",
        linked,
        ps.score(),
        ps.totalAppearances,
        ps.uniqueDirs,
        ps.avgFiles(),
        humanSize((long) ps.avgSizeBytes()),
        entry.getKey());
  }

  private static String humanSize(long bytes) {
    if (bytes < 1_024) return bytes + " B";
    if (bytes < 1_024 * 1_024) return String.format("%.1f KB", bytes / 1_024.0);
    if (bytes < 1_024 * 1_024 * 1_024) return String.format("%.1f MB", bytes / (1_024.0 * 1_024));
    return String.format("%.1f GB", bytes / (1_024.0 * 1_024 * 1_024));
  }
}
