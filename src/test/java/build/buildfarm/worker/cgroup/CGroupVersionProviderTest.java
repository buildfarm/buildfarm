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

package build.buildfarm.worker.cgroup;

import static build.buildfarm.common.base.System.isWindows;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class CGroupVersionProviderTest {

  @Before
  public void setup() throws IOException {
    Assume.assumeFalse(isWindows());
  }

  private void testDetectCGroups(String type, CGroupVersion detectedVersion) throws IOException {
    // Create a mock FileStore that returns `type` as the type
    FileStore mockFileStore = mock(FileStore.class);
    when(mockFileStore.type()).thenReturn(type);

    try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class);
        MockedStatic<Path> mockedPath = Mockito.mockStatic(Path.class)) {

      // Mock Path.of to return a path we can control
      Path mockPath = mock(Path.class);
      mockedPath.when(() -> Path.of("/sys/fs/cgroup")).thenReturn(mockPath);
      // Make the toFile().exists() return false
      java.io.File mockFile = mock(java.io.File.class);
      when(mockPath.toFile()).thenReturn(mockFile);
      when(mockFile.exists()).thenReturn(true);
      when(mockFile.isDirectory()).thenReturn(true);

      // Mock the Files.getFileStore method to return our mock FileStore
      mockedFiles.when(() -> Files.getFileStore(mockPath)).thenReturn(mockFileStore);

      // Create the provider and call the get method
      CGroupVersionProvider provider = new CGroupVersionProvider();
      CGroupVersion result = provider.get();

      // Verify that it correctly detected CGroups
      assertEquals(detectedVersion, result);

      // Verify interactions with our mocks
      mockedFiles.verify(() -> Files.getFileStore(mockPath));
      verify(mockFileStore, times(1)).type();
      mockedPath.verify(() -> Path.of("/sys/fs/cgroup"));
      verify(mockPath, times(2)).toFile();
      verify(mockFile).exists();
      verify(mockFile).isDirectory();

      verifyNoMoreInteractions(mockPath, mockFile, mockFileStore);
      mockedFiles.verifyNoMoreInteractions();
      mockedPath.verifyNoMoreInteractions();
    }
  }

  @Test
  public void testDetectCGroupsV1() throws IOException {
    testDetectCGroups("cgroup", CGroupVersion.CGROUPS_V1);
  }

  @Test
  public void testDetectCGroupsV1tmpfs() throws IOException {
    /* Sometimes it is mounted as tmpfs inside k8s */
    testDetectCGroups("tmpfs", CGroupVersion.CGROUPS_V1);
  }

  @Test
  public void testDetectCGroupsV2() throws IOException {
    testDetectCGroups("cgroup2", CGroupVersion.CGROUPS_V2);
  }

  @Test
  public void testNoCGroupsWhenDirectoryDoesntExist() {
    try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class);
        MockedStatic<Path> mockedPath = Mockito.mockStatic(Path.class)) {

      // Mock Path.of to return a path we can control
      Path mockPath = mock(Path.class);
      mockedPath.when(() -> Path.of("/sys/fs/cgroup")).thenReturn(mockPath);

      // Make the toFile().exists() return false
      java.io.File mockFile = mock(java.io.File.class);
      when(mockPath.toFile()).thenReturn(mockFile);
      when(mockFile.exists()).thenReturn(false);

      // Create the provider and call the get method
      CGroupVersionProvider provider = new CGroupVersionProvider();
      CGroupVersion result = provider.get();

      // Verify that it correctly detected no CGroups
      assertEquals(CGroupVersion.NONE, result);

      // Verify interactions with our mocks
      mockedPath.verify(() -> Path.of("/sys/fs/cgroup"));
      verify(mockPath).toFile();
      verify(mockFile).exists();
      // We should never check isDirectory if exists() returns false
      verify(mockFile, Mockito.never()).isDirectory();
      // We should never call Files.getFileStore if the directory doesn't exist
      mockedFiles.verifyNoInteractions();

      mockedPath.verifyNoMoreInteractions();
    }

    // No need to verify mockFile and mockPath outside of the try block
    // as they are only referenced within the scope of the MockedStatic resources
  }

  @Test
  public void testIOExceptionHandling() {
    try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class);
        MockedStatic<Path> mockedPath = Mockito.mockStatic(Path.class)) {

      // Mock Path.of to return a path we can control
      Path mockPath = mock(Path.class);
      mockedPath.when(() -> Path.of("/sys/fs/cgroup")).thenReturn(mockPath);

      // Make the toFile().exists() return true
      java.io.File mockFile = mock(java.io.File.class);
      when(mockPath.toFile()).thenReturn(mockFile);
      when(mockFile.exists()).thenReturn(true);
      when(mockFile.isDirectory()).thenReturn(true);

      // Make Files.getFileStore throw an IOException
      mockedFiles
          .when(() -> Files.getFileStore(mockPath))
          .thenThrow(new IOException("Test IOException"));

      // Create the provider and call the get method
      CGroupVersionProvider provider = new CGroupVersionProvider();
      CGroupVersion result = provider.get();

      // Verify that it correctly falls back to NONE when there's an exception
      assertEquals(CGroupVersion.NONE, result);

      // Verify interactions with our mocks
      mockedPath.verify(() -> Path.of("/sys/fs/cgroup"));
      verify(mockPath, times(2)).toFile();
      verify(mockFile).exists();
      verify(mockFile).isDirectory();
      mockedFiles.verify(() -> Files.getFileStore(mockPath));

      mockedPath.verifyNoMoreInteractions();
      mockedFiles.verifyNoMoreInteractions();
    }
  }
}
