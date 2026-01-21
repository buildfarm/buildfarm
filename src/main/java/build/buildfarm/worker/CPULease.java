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

import static com.google.common.base.Preconditions.checkArgument;

import build.buildfarm.common.Claim.Lease;
import build.buildfarm.common.Claim.Stage;

public class CPULease implements Lease {
  public static final String RESOURCE_NAME = "cpu.shares";
  private int balance = 0;

  CPULease(int amount, Market market) throws InterruptedException {
    Order order = market.buy(amount, this::accumulate);
    try {
      synchronized (this) {
        while (balance != amount) {
          wait();
        }
      }
    } catch (InterruptedException e) {
      order.cancel();
      // objectively finally, but we could still be interrupted while waiting for the last item
      market.sell(balance);
      throw e;
    }
  }

  public synchronized void accumulate(int n) {
    checkArgument(n >= 0);
    balance += n;
    notifyAll();
  }

  public synchronized void deplete(int n) {
    checkArgument(n >= 0);
    balance -= n;
  }

  @Override
  public String name() {
    return RESOURCE_NAME;
  }

  @Override
  public synchronized int amount() {
    return balance;
  }

  public Stage stage() {
    return Stage.EXECUTE_ACTION_STAGE;
  }
}
