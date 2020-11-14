// Copyright 2020 The Bazel Authors. All rights reserved.
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

/*
  This program is used as a benchmarking test for IO functions.
  When performing IO-related operations, Java's abstractions may come at a performance cost
  (particularly in the CASFileCache due to its use of xfs, filenames, and hardlinking).

  These tests are used to benchmark the various IO utilities within buildfarm.
  Some of these utilities use the the default Java standard library.
  Others use JNA-Posix for native access to posix related functions.
  Performance and portability trade-offs can be evaluated based on these benchmarks.

  To use:
  The first argument to this java_binary is the test directory to run benchmarks on.
  The remaining arguments are forwarded to JNH.

  Note that your operating system is going to cache pages, dentries, and inodes automatically.
  A better way to benchmark these functions is to ensure your cache is cleared before each call.
  This could be done with:
  sync; echo 3 > /proc/sys/vm/drop_caches
*/

package build.buildfarm;

import static build.buildfarm.common.io.Utils.getInode;
import static build.buildfarm.common.io.Utils.isDir;
import static build.buildfarm.common.io.Utils.jnrGetInode;
import static build.buildfarm.common.io.Utils.jnrIsDir;
import static build.buildfarm.common.io.Utils.jnrReaddir;
import static build.buildfarm.common.io.Utils.jnrStatNullable;
import static build.buildfarm.common.io.Utils.posixReaddir;
import static build.buildfarm.common.io.Utils.posixStatNullable;
import static build.buildfarm.common.io.Utils.readdir;
import static build.buildfarm.common.io.Utils.statNullable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import jnr.posix.util.DefaultPOSIXHandler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class IOBenchmark {

  /*
    JMH provides no way to share data between the benchmarks.
    We share the benchmarking path to use via a temp file.
    The benchmarking test is written to before the benchmark runs.
    Each benchmark reads the path before executing the benchmark.
  */
  private static void WriteTestDir(Path testDir) throws Exception {

    Path fileName = Paths.get(System.getProperty("java.io.tmpdir"), "testDir");
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName.toString()));
    System.out.println("running IO benchmarks on the following directory: " + testDir.toString());
    writer.write(testDir.toString());
    writer.close();
  }

  private static Path ReadTestDir() throws Exception {
    Path fileName = Paths.get(System.getProperty("java.io.tmpdir"), "testDir");
    BufferedReader reader = new BufferedReader(new FileReader(fileName.toString()));
    Path testDir = Paths.get(reader.readLine());
    reader.close();
    return testDir;
  }

  /*
    Settings used by each benchmark
  */
  @State(Scope.Benchmark)
  public static class IOState {
    public Path testDir = null;
    public String testDirStr = null;
    public POSIX posix = null;
    public FileStore fileStore;

    @Setup(Level.Invocation)
    public void setUp() throws Exception {
      testDir = ReadTestDir();
      testDirStr = testDir.toString();
      posix = POSIXFactory.getPOSIX(new DefaultPOSIXHandler(), true);
      fileStore = Files.getFileStore(testDir);
    }
  }

  /*
    Benchmark stat calls
  */
  @Benchmark
  public static Object benchmark_statNullable(IOState state) throws Exception {
    return statNullable(state.testDir, false, state.fileStore);
  }

  @Benchmark
  public static Object benchmark_posixStatNullable(IOState state) throws Exception {
    return posixStatNullable(state.testDir, false);
  }

  @Benchmark
  public static Object benchmark_jnrStatNullable(IOState state) throws Exception {
    return jnrStatNullable(state.posix, state.testDirStr);
  }

  /*
    Benchmark getting inodes
  */
  @Benchmark
  public static Object benchmark_getInode(IOState state) throws Exception {
    return getInode(state.testDir);
  }

  @Benchmark
  public static Object benchmark_jnrGetInode(IOState state) throws Exception {
    return jnrGetInode(state.posix, state.testDirStr);
  }

  /*
    Benchmark getting dirents
  */
  @Benchmark
  public static Object benchmark_readdir(IOState state) throws Exception {
    return readdir(state.testDir, false, state.fileStore);
  }

  @Benchmark
  public static Object benchmark_posixReaddir(IOState state) throws Exception {
    return posixReaddir(state.testDir, false);
  }

  @Benchmark
  public static Object benchmark_jnrReaddir(IOState state) throws Exception {
    return jnrReaddir(state.posix, state.testDir);
  }

  /*
    Benchmark is directory calls
  */
  @Benchmark
  public static Object benchmark_isDir(IOState state) throws Exception {
    return isDir(state.testDirStr);
  }

  public static Object benchmark_jnrIsDir(IOState state) throws Exception {
    return jnrIsDir(state.posix, state.testDirStr);
  }

  public static void main(String[] args) throws Exception {

    // Get the test directory to run io benchmarks on.
    // JMH provides no way to share data between the benchmarks.
    // We share the benchmarking path via a temp file.
    Path testDir = Paths.get(args[0]);
    WriteTestDir(testDir);

    // Call the JMH benchmarker with the remaining arguments.
    String[] jmhArguments = Arrays.copyOfRange(args, 1, args.length);
    org.openjdk.jmh.Main.main(jmhArguments);
  }
}
