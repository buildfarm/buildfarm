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

package build.buildfarm.worker;

import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.BlobNotFoundException;
import build.buildfarm.v1test.Digest;
import build.buildfarm.worker.ExecDirException.ViolationException;
import com.google.rpc.PreconditionFailure.Violation;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExecDirExceptionTest {
  private static final Digest DIGEST = Digest.newBuilder().setHash("hash").setSize(1).build();

  @Test
  public void directBlobNotFoundIsMissing() {
    ViolationException e =
        new ViolationException(
            DIGEST,
            Path.of("file"),
            /* isExecutable= */ false,
            new BlobNotFoundException("file", new IOException("not found")));
    assertThat(e.getViolation().getType()).isEqualTo(VIOLATION_TYPE_MISSING);
  }

  @Test
  public void noSuchFileIsMissing() {
    ViolationException e =
        new ViolationException(
            DIGEST, Path.of("file"), /* isExecutable= */ false, new NoSuchFileException("file"));
    assertThat(e.getViolation().getType()).isEqualTo(VIOLATION_TYPE_MISSING);
  }

  @Test
  public void wrappedBlobNotFoundIsMissing() {
    // Issue #1964: a BlobNotFoundException, wrapped (possibly at multiple layers) by the stream
    // class reporting exceptions, must still be classified as a MISSING violation rather than an
    // INVALID precondition failure.
    Throwable wrapped =
        new IOException(
            "outer",
            new IOException(
                "inner",
                new BlobNotFoundException(
                    "file", new RejectedExecutionException("could not retry read"))));
    ViolationException e =
        new ViolationException(DIGEST, Path.of("file"), /* isExecutable= */ false, wrapped);
    Violation violation = e.getViolation();
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_MISSING);
    assertThat(violation.getDescription()).contains("was not found in the CAS");
  }

  @Test
  public void unrelatedCauseIsInvalid() {
    ViolationException e =
        new ViolationException(
            DIGEST,
            Path.of("file"),
            /* isExecutable= */ false,
            new IOException("some other failure"));
    Violation violation = e.getViolation();
    assertThat(violation.getType()).isEqualTo(VIOLATION_TYPE_INVALID);
    assertThat(violation.getDescription()).isEqualTo("some other failure");
  }
}
