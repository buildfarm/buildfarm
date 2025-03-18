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

package build.buildfarm.common.blake3;

import static build.buildfarm.common.base.System.isDarwin;
import static build.buildfarm.common.base.System.isWindows;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.io.ByteStreams;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import javax.annotation.Nullable;
import lombok.extern.java.Log;

/** Generic code to interact with the platform-specific JNI code bundle. */
@Log
public final class JniLoader {
  @Nullable private static final Throwable JNI_LOAD_ERROR;

  static {
    Throwable jniLoadError;
    try {
      if (isDarwin()) {
        loadLibrary("main/native/libblake3_jni.dylib");
      } else if (isWindows()) {
        loadLibrary("main/native/blake3_jni.dll");
      } else { // presumed soloader capable
        loadLibrary("main/native/libblake3_jni.so");
      }
      jniLoadError = null;
    } catch (IOException | UnsatisfiedLinkError e) {
      log.log(Level.WARNING, "Failed to load JNI library", e);
      jniLoadError = e;
    }
    JNI_LOAD_ERROR = jniLoadError;
  }

  /**
   * Loads a resource as a shared library.
   *
   * @param resourceName the name of the shared library to load, specified as a slash-separated
   *     relative path within the JAR with at least two components
   * @throws IOException if the resource cannot be extracted or loading the library fails for any
   *     other reason
   */
  @SuppressWarnings("PMD.UseProperClassLoader")
  private static void loadLibrary(String resourceName) throws IOException {
    Path dir = null;
    Path tempFile = null;
    try {
      dir = Files.createTempDirectory("buildfarm-jni.");
      int slash = resourceName.lastIndexOf('/');
      checkArgument(slash != -1, "resourceName must contain two path components");
      tempFile = dir.resolve(resourceName.substring(slash + 1));

      ClassLoader loader = JniLoader.class.getClassLoader();
      try (InputStream resource = loader.getResourceAsStream(resourceName)) {
        if (resource == null) {
          throw new UnsatisfiedLinkError("Resource " + resourceName + " not in JAR");
        }
        try (OutputStream diskFile = new FileOutputStream(tempFile.toString())) {
          ByteStreams.copy(resource, diskFile);
        }
      }

      System.load(tempFile.toString());

      // Remove the temporary file now that we have loaded it. If we keep it short-lived, we can
      // avoid the file system from persisting it to disk, avoiding an unnecessary cost.
      //
      // Unfortunately, we cannot do this on Windows because the DLL remains open and we don't have
      // a way to specify FILE_SHARE_DELETE in the System.load() call.
      if (!isWindows()) {
        Files.delete(tempFile);
        tempFile = null;
        Files.delete(dir);
      }
    } catch (IOException e) {
      try {
        if (tempFile != null) {
          Files.deleteIfExists(tempFile);
        }
        if (dir != null) {
          Files.delete(dir);
        }
      } catch (IOException e2) {
        // Nothing else we can do. Rely on "delete on exit" to try clean things up later on.
      }
      throw e;
    }
  }

  private JniLoader() {}

  /**
   * Triggers the load of the JNI bundle in a platform-independent basis.
   *
   * <p>This does <b>not</b> fail if the JNI bundle cannot be loaded because there are scenarios in
   * which we want to run Bazel without JNI (e.g. during bootstrapping) or are able to fall back to
   * an alternative implementation (e.g. in some filesystem implementations).
   *
   * <p>Callers can check if the JNI bundle was successfully loaded via {@link #isJniAvailable()}
   * and obtain the load error via {@link #getJniLoadError()}.
   */
  public static void loadJni() {}

  /** Returns whether the JNI bundle was successfully loaded. */
  public static boolean isJniAvailable() {
    return JNI_LOAD_ERROR == null;
  }

  /** Returns the exception thrown while loading the JNI bundle, if it failed. */
  @Nullable
  public static Throwable getJniLoadError() {
    return JNI_LOAD_ERROR;
  }
}
