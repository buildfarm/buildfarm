// Copyright 2017 The Bazel Authors. All rights reserved.
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


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class CasReplicationStage extends PipelineStage {
  private static final Logger logger = Logger.getLogger(CasReplicationStage.class.getName());

  private final BlockingQueue<OperationContext> queue = new ArrayBlockingQueue<>(1);

  public CasReplicationStage() {
    super("CasReplicationStage", null, new PipelineStage.NullStage(), null);
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  public OperationContext take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {}

  @Override
  protected OperationContext tick(OperationContext operationContext) throws InterruptedException {
    return OperationContext.newBuilder().build();
  }

  @Override
  protected void after(OperationContext operationContext) {}
}
