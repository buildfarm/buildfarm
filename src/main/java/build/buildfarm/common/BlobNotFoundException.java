// Copyright 2024 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.common;

import java.io.IOException;

/**
 * Exception thrown when a blob cannot be found in the Content Addressable Storage (CAS).
 *
 * <p>This exception is specifically used in Buildfarm's distributed build system to indicate that a
 * requested blob (identified by its digest) is not available in the CAS. Blobs in the CAS represent
 * various build artifacts including source files, intermediate build outputs, and final build
 * results that are stored and retrieved by their cryptographic hash.
 *
 * <p>Common scenarios where this exception is thrown include:
 *
 * <ul>
 *   <li>When a ByteStream gRPC read operation fails due to execution service rejection, indicating
 *       the blob cannot be retrieved from remote storage
 *   <li>During execution directory setup when required input files or dependencies referenced by
 *       digest are missing from the CAS
 *   <li>When build artifacts that should be available based on previous operations are no longer
 *       accessible due to storage cleanup or network issues
 * </ul>
 *
 * <p>This exception extends {@link IOException} as blob retrieval failures are fundamentally I/O
 * related operations. It is typically caught and handled by converting it into appropriate build
 * violation types (e.g., VIOLATION_TYPE_MISSING) for reporting to build clients.
 *
 * <p>The exception message typically contains the resource name or digest of the missing blob, and
 * the cause contains the underlying reason for the failure (e.g., network timeout, storage service
 * unavailability, or execution service rejection).
 *
 * @see build.buildfarm.common.grpc.ByteStreamHelper
 * @see build.buildfarm.worker.ExecDirException
 */
public class BlobNotFoundException extends IOException {
  public BlobNotFoundException(String name, Throwable cause) {
    super(name, cause);
  }
}
