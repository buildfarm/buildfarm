// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.buildfarm.cas.cfc.LRUDB.SizeEntry;
import com.google.common.collect.Iterables;
import com.google.common.jimfs.Jimfs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TextLRUDBTest {
  private TextLRUDB textLRUDB;
  private FileSystem fileSystem;
  private Path rootDir;

  @Before
  public void setUp() {
    textLRUDB = new TextLRUDB();
    fileSystem = Jimfs.newFileSystem();
    rootDir = Iterables.getFirst(fileSystem.getRootDirectories(), null);
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.close();
  }

  @Test
  public void entries_withValidData_parsesCorrectly() throws IOException {
    // Arrange
    String data = "key1,100\nkey2,200\nkey3,300\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).hasSize(3);
    assertThat(entryList.get(0)).isEqualTo(new SizeEntry("key1", 100L));
    assertThat(entryList.get(1)).isEqualTo(new SizeEntry("key2", 200L));
    assertThat(entryList.get(2)).isEqualTo(new SizeEntry("key3", 300L));
  }

  @Test
  public void entries_withEmptyReader_returnsEmptyIterable() throws IOException {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader(""));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).isEmpty();
  }

  @Test
  public void entries_withSingleEntry_parsesCorrectly() throws IOException {
    // Arrange
    String data = "singleKey,42\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).hasSize(1);
    assertThat(entryList.get(0)).isEqualTo(new SizeEntry("singleKey", 42L));
  }

  @Test
  public void entries_withZeroSize_parsesCorrectly() throws IOException {
    // Arrange
    String data = "zeroKey,0\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).hasSize(1);
    assertThat(entryList.get(0)).isEqualTo(new SizeEntry("zeroKey", 0L));
  }

  @Test
  public void entries_withLargeSize_parsesCorrectly() throws IOException {
    // Arrange
    String data = "largeKey,9223372036854775807\n"; // Long.MAX_VALUE
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).hasSize(1);
    assertThat(entryList.get(0)).isEqualTo(new SizeEntry("largeKey", Long.MAX_VALUE));
  }

  @Test
  public void entries_withSpecialCharactersInKey_parsesCorrectly() throws IOException {
    // Arrange
    String data = "key-with_special.chars/path,123\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    List<SizeEntry> entryList = new ArrayList<>();
    for (SizeEntry entry : entries) {
      entryList.add(entry);
    }

    // Assert
    assertThat(entryList).hasSize(1);
    assertThat(entryList.get(0)).isEqualTo(new SizeEntry("key-with_special.chars/path", 123L));
  }

  @Test
  public void entries_withMissingComma_throwsIllegalStateException() throws IOException {
    // Arrange
    String data = "invalidline\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act & Assert
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    Iterator<SizeEntry> iterator = entries.iterator();

    assertThrows(
        NoSuchElementException.class,
        () -> {
          iterator.next();
        });
  }

  @Test
  public void entries_withInvalidNumber_throwsNumberFormatException() throws IOException {
    // Arrange
    String data = "key,notanumber\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act & Assert
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    Iterator<SizeEntry> iterator = entries.iterator();

    assertThrows(
        NoSuchElementException.class,
        () -> {
          iterator.next();
        });
  }

  @Test
  public void entries_iterator_hasNextBehavior() throws IOException {
    // Arrange
    String data = "key1,100\nkey2,200\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    Iterator<SizeEntry> iterator = entries.iterator();

    // Assert
    assertThat(iterator.hasNext()).isTrue();
    SizeEntry first = iterator.next();
    assertThat(first).isEqualTo(new SizeEntry("key1", 100L));

    assertThat(iterator.hasNext()).isTrue();
    SizeEntry second = iterator.next();
    assertThat(second).isEqualTo(new SizeEntry("key2", 200L));

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void entries_iterator_nextWithoutHasNext_throwsNoSuchElementException()
      throws IOException {
    // Arrange
    BufferedReader reader = new BufferedReader(new StringReader(""));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    Iterator<SizeEntry> iterator = entries.iterator();

    // Assert
    assertThrows(
        NoSuchElementException.class,
        () -> {
          iterator.next();
        });
  }

  @Test
  public void entries_iterator_multipleHasNextCalls() throws IOException {
    // Arrange
    String data = "key1,100\n";
    BufferedReader reader = new BufferedReader(new StringReader(data));

    // Act
    Iterable<SizeEntry> entries = textLRUDB.entries(reader);
    Iterator<SizeEntry> iterator = entries.iterator();

    // Assert - multiple hasNext() calls should not advance the iterator
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.hasNext()).isTrue();

    SizeEntry entry = iterator.next();
    assertThat(entry).isEqualTo(new SizeEntry("key1", 100L));

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  public void save_withValidEntries_writesCorrectFormat() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("test.txt");
    List<SizeEntry> entries =
        List.of(
            new SizeEntry("key1", 100L), new SizeEntry("key2", 200L), new SizeEntry("key3", 300L));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("key1,100\nkey2,200\nkey3,300\n");
  }

  @Test
  public void save_withEmptyIterator_createsEmptyFile() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("empty.txt");
    List<SizeEntry> entries = List.of();
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEmpty();
  }

  @Test
  public void save_withSingleEntry_writesCorrectly() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("single.txt");
    List<SizeEntry> entries = List.of(new SizeEntry("onlyKey", 42L));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("onlyKey,42\n");
  }

  @Test
  public void save_withZeroSizeEntry_writesCorrectly() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("zero.txt");
    List<SizeEntry> entries = List.of(new SizeEntry("zeroKey", 0L));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("zeroKey,0\n");
  }

  @Test
  public void save_withLargeSizeEntry_writesCorrectly() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("large.txt");
    List<SizeEntry> entries = List.of(new SizeEntry("largeKey", Long.MAX_VALUE));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("largeKey,9223372036854775807\n");
  }

  @Test
  public void save_withSpecialCharactersInKey_writesCorrectly() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("special.txt");
    List<SizeEntry> entries = List.of(new SizeEntry("key-with_special.chars/path", 123L));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("key-with_special.chars/path,123\n");
  }

  @Test
  public void roundTrip_saveAndLoad_preservesData() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("roundtrip.txt");
    List<SizeEntry> originalEntries =
        List.of(
            new SizeEntry("key1", 100L), new SizeEntry("key2", 200L), new SizeEntry("key3", 300L));

    // Act - Save
    textLRUDB.save(originalEntries.iterator(), tempFile);

    // Act - Load
    BufferedReader reader = Files.newBufferedReader(tempFile);
    Iterable<SizeEntry> loadedEntries = textLRUDB.entries(reader);
    List<SizeEntry> loadedList = new ArrayList<>();
    for (SizeEntry entry : loadedEntries) {
      loadedList.add(entry);
    }

    // Assert
    assertThat(loadedList).containsExactlyElementsIn(originalEntries).inOrder();
  }

  @Test
  public void save_overwritesExistingFile() throws IOException {
    // Arrange
    Path tempFile = rootDir.resolve("overwrite.txt");
    Files.writeString(tempFile, "existing content");

    List<SizeEntry> entries = List.of(new SizeEntry("newKey", 999L));
    Iterator<SizeEntry> iterator = entries.iterator();

    // Act
    textLRUDB.save(iterator, tempFile);

    // Assert
    String content = Files.readString(tempFile);
    assertThat(content).isEqualTo("newKey,999\n");
  }
}
