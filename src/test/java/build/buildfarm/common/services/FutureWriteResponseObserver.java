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

package build.buildfarm.common.services;

import com.google.bytestream.ByteStreamProto.WriteResponse;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;

class FutureWriteResponseObserver implements StreamObserver<WriteResponse> {
  private final SettableFuture<WriteResponse> future = SettableFuture.create();

  @Override
  public void onNext(WriteResponse response) {
    future.set(response);
  }

  @Override
  public void onCompleted() {}

  @Override
  public void onError(Throwable t) {
    future.setException(t);
  }

  public boolean isDone() {
    return future.isDone();
  }

  public WriteResponse get() throws ExecutionException, InterruptedException {
    return future.get();
  }
}
