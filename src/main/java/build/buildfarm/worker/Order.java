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

import java.util.function.Consumer;

class Order {
  static final Order COMPLETE = new Order(0, available -> {});
  private final Consumer<Integer> onBuy;
  private int balance;

  Order(int balance, Consumer<Integer> onBuy) {
    this.onBuy = onBuy;
    this.balance = balance;
  }

  synchronized void cancel() {
    balance = 0;
  }

  // returns the number of shares bought from the available pool
  synchronized int buy(int available) {
    if (balance == 0 || available == 0) {
      return 0;
    }
    checkArgument(available > 0);
    int sharesToBuy = Math.min(available, balance);
    balance -= sharesToBuy;
    onBuy.accept(sharesToBuy);
    return sharesToBuy;
  }

  synchronized int balance() {
    return balance;
  }
}
