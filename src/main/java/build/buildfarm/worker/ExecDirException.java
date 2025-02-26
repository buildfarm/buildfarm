// Copyright 2023 The Buildfarm Authors. All rights reserved.
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

import static build.buildfarm.common.Errors.MISSING_INPUT;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_INVALID;
import static build.buildfarm.common.Errors.VIOLATION_TYPE_MISSING;
import static java.util.logging.Level.SEVERE;

import build.buildfarm.cas.cfc.PutDirectoryException;
import build.buildfarm.common.BlobNotFoundException;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.v1test.Digest;
import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.PreconditionFailure.Violation;
import com.google.rpc.Status;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.java.Log;

@Log
public class ExecDirException extends IOException {
  private final Path path;
  private final List<Throwable> exceptions;

  public static class ViolationException extends Exception {
    private final Digest digest;
    private final Path path;
    private final boolean isExecutable;

    public ViolationException(Digest digest, Path path, boolean isExecutable, Throwable cause) {
      super(cause);
      this.digest = digest;
      this.path = path;
      this.isExecutable = isExecutable;
    }

    private static String getDescription(Path path, boolean isExecutable) {
      if (path != null) {
        return "The file `/" + path + (isExecutable ? "*" : "") + "` was not found in the CAS.";
      }
      return MISSING_INPUT;
    }

    static void toViolation(
        Violation.Builder violation, Throwable cause, Path path, boolean isExecutable) {
      if (cause instanceof NoSuchFileException || cause instanceof BlobNotFoundException) {
        violation
            .setType(VIOLATION_TYPE_MISSING)
            .setDescription(getDescription(path, isExecutable));
      } else {
        violation.setType(VIOLATION_TYPE_INVALID).setDescription(cause.getMessage());
      }
    }

    public Violation getViolation() {
      Violation.Builder violation = Violation.newBuilder();
      toViolation(violation, getCause(), path, isExecutable);
      violation.setSubject("blobs/" + DigestUtil.toString(digest));
      return violation.build();
    }
  }

  private static String getErrorMessage(Path path, List<Throwable> exceptions) {
    return String.format("%s: %d %s: %s", path, exceptions.size(), "exceptions", exceptions);
  }

  public ExecDirException(Path path, List<Throwable> exceptions) {
    // When printing the exception, show the captured sub-exceptions.
    super(getErrorMessage(path, exceptions));
    this.path = path;
    this.exceptions = exceptions;
    for (Throwable exception : exceptions) {
      addSuppressed(exception);
    }
  }

  Path getPath() {
    return path;
  }

  List<Throwable> getExceptions() {
    return exceptions;
  }

  Status.Builder toStatus(Status.Builder status) {
    status.setCode(Code.FAILED_PRECONDITION.getNumber());

    // aggregate into a single preconditionFailure
    PreconditionFailure.Builder preconditionFailure = PreconditionFailure.newBuilder();
    for (Throwable exception : exceptions) {
      if (exception instanceof ViolationException violationException) {
        preconditionFailure.addViolations(violationException.getViolation());
      } else if (exception instanceof PutDirectoryException putDirException) {
        for (Throwable putDirCause : putDirException.getExceptions()) {
          if (putDirCause instanceof IOException) {
            Violation.Builder violation = preconditionFailure.addViolationsBuilder();
            ViolationException.toViolation(
                violation, putDirCause, /* path= */ null, /* isExecutable= */ false);
            if (putDirCause instanceof NoSuchFileException) {
              violation.setSubject("blobs/" + putDirCause.getMessage());
            } else {
              log.log(SEVERE, "unrecognized put dir cause exception", putDirCause);
              violation.setSubject("blobs/" + DigestUtil.toString(putDirException.getDigest()));
            }
          } else {
            log.log(SEVERE, "unrecognized put dir exception", putDirCause);
            status.setCode(Code.INTERNAL.getNumber());
          }
        }
      } else {
        log.log(SEVERE, "unrecognized exec dir exception", exception);
        status.setCode(Code.INTERNAL.getNumber());
      }
    }
    if (preconditionFailure.getViolationsCount() > 0) {
      status.addDetails(Any.pack(preconditionFailure.build()));
    }

    return status;
  }
}
