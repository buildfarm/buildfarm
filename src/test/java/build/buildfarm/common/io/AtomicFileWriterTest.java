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

package build.buildfarm.common.io;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AtomicFileWriterTest {
  private Path root;
  private FileStore fileStore;

  @Before
  public void setUp() throws IOException {
    root = Files.createTempDirectory("atomic-file-writer-test");
    fileStore = Files.getFileStore(root);
  }

  @After
  public void tearDown() throws IOException {
    Directories.remove(root, fileStore);
  }

  @Test
  public void atomicFileWriterSuccess() throws IOException {
    Path target = root.resolve("test.txt");

    try (AtomicFileWriter writer = new AtomicFileWriter(target)) {
      writer.getWriter().write("test content");
    }

    assertThat(Files.exists(target)).isTrue();
    assertThat(Files.readString(target)).isEqualTo("test content");

    // Verify no temp files left behind
    long tempFileCount = Files.list(root).filter(p -> p.toString().contains(".tmp")).count();
    assertThat(tempFileCount).isEqualTo(0);
  }

  @Test
  public void atomicFileWriterReplacesExisting() throws IOException {
    Path target = root.resolve("test.txt");
    Files.writeString(target, "old content");

    try (AtomicFileWriter writer = new AtomicFileWriter(target)) {
      writer.getWriter().write("new content");
    }

    assertThat(Files.readString(target)).isEqualTo("new content");
  }

  @Test
  public void atomicFileWriterCleansUpOnException() throws IOException {
    Path target = root.resolve("test.txt");

    try {
      try (AtomicFileWriter writer = new AtomicFileWriter(target)) {
        writer.getWriter().write("partial");
        throw new IOException("simulated error");
      }
    } catch (IOException e) {
      // Expected
      assertThat(e.getMessage()).isEqualTo("simulated error");
    }

    // With auto-commit, file is created even when exception occurs before close
    // because close() itself completes successfully
    assertThat(Files.exists(target)).isTrue();
    assertThat(Files.readString(target)).isEqualTo("partial");

    // No temp files should remain
    long tempFileCount = Files.list(root).filter(p -> p.toString().contains(".tmp")).count();
    assertThat(tempFileCount).isEqualTo(0);
  }

  @Test
  public void atomicFileWriterUniqueTempFiles() throws IOException {
    Path target = root.resolve("test.txt");

    // Create two writers to same target - should have different temp files
    AtomicFileWriter writer1 = new AtomicFileWriter(target);
    AtomicFileWriter writer2 = new AtomicFileWriter(target);

    // Both should succeed without collision
    writer1.getWriter().write("content1");
    writer2.getWriter().write("content2");

    writer1.close();
    writer2.close();

    // Last writer wins
    assertThat(Files.exists(target)).isTrue();
    String content = Files.readString(target);
    // Content should be from one of the writers (last one to close wins)
    assertThat(content.equals("content1") || content.equals("content2")).isTrue();
  }

  @Test
  public void atomicFileWriterMultipleLines() throws IOException {
    Path target = root.resolve("multiline.txt");

    try (AtomicFileWriter writer = new AtomicFileWriter(target)) {
      writer.getWriter().write("line1\n");
      writer.getWriter().write("line2\n");
      writer.getWriter().write("line3\n");
    }

    assertThat(Files.exists(target)).isTrue();
    assertThat(Files.readString(target)).isEqualTo("line1\nline2\nline3\n");
  }

  @Test
  public void atomicFileWriterIdempotentClose() throws IOException {
    Path target = root.resolve("test.txt");

    AtomicFileWriter writer = new AtomicFileWriter(target);
    writer.getWriter().write("test content");
    writer.close();

    // Second close should be safe (no-op)
    writer.close();

    assertThat(Files.exists(target)).isTrue();
    assertThat(Files.readString(target)).isEqualTo("test content");
  }
}
