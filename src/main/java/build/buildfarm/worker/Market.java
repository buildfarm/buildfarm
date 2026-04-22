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

import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.Claim;
import build.buildfarm.common.Resource;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.java.Log;

@Log
public class Market implements Runnable, Resource {
  private static final long CYCLE_WAIT_S = 1;
  private static final int CYCLE_WAIT_NS = 0;

  private final BlockingQueue<Order> orders = new LinkedBlockingQueue<>(); // buys
  private final Thread broker = new Thread(this);
  private int balance = 0;
  private Consumer<Integer> onChangeBalance = balance -> {};

  public synchronized int balance() {
    return balance;
  }

  // set this once, avoid reset
  public void onChangeBalance(Consumer<Integer> onChangeBalance) {
    this.onChangeBalance = onChangeBalance;
  }

  Order buy(int amount, Consumer<Integer> onBuy) {
    Order order = new Order(amount, onBuy);
    synchronized (this) {
      orders.add(order);
      notifyAll();
    }
    return order;
  }

  private boolean clearCancelledOrders() {
    Iterator<Order> iter = orders.iterator();
    boolean removed = false;
    while (iter.hasNext()) {
      Order o = iter.next();
      if (o.balance() == 0) {
        iter.remove();
        removed = true;
      }
    }
    return removed;
  }

  @GuardedBy("this")
  private void waitCycle() throws InterruptedException {
    wait(CYCLE_WAIT_S, CYCLE_WAIT_NS);
  }

  @Override
  public void run() {
    try {
      int totalBought = 0;
      for (; ; ) {
        int available = balance();
        while (available > totalBought) {
          Order order;
          if (totalBought == 0) {
            order = orders.take();
          } else {
            order = orders.poll();
            if (order == null) {
              synchronized (this) {
                balance = available = balance - totalBought;
                totalBought = 0;
              }
              onChangeBalance.accept(available);
              continue;
            }
          }
          int bought = order.buy(available - totalBought);
          totalBought += bought;
          if (bought != 0) {
            if (available == totalBought) {
              synchronized (this) {
                balance = available = balance - totalBought;
              }
              onChangeBalance.accept(available);
              totalBought = 0;
            }
            // replace order if we bought something for it
            // 0 balances will release themselves after the next iteration
            orders.add(order);
          }
        }
        // out of buy orders or at 0 balance
        synchronized (this) {
          balance -= totalBought;
          while (orders.isEmpty() || balance == 0) {
            // purge the rolls of cancelled orders, cycling until none are reported
            if (balance != 0 || !clearCancelledOrders()) {
              waitCycle();
            }
          }
        }
      }
    } catch (InterruptedException e) {
      // ignore
    } catch (RuntimeException e) {
      log.log(SEVERE, "Market run threw exception", e);
    }
  }

  public void start() {
    broker.start();
  }

  public void stop() throws InterruptedException {
    broker.interrupt();
    broker.join();
  }

  public void sell(int amount) {
    if (amount != 0) {
      int available = sellSyncPositive(amount);
      onChangeBalance.accept(available);
    }
  }

  synchronized int sellSyncPositive(int amount) {
    balance += amount;
    notifyAll();
    return balance;
  }

  @Override
  public Claim.Stage stage() {
    return Claim.Stage.EXECUTE_ACTION_STAGE;
  }

  @Override
  public boolean tryAcquire(int amount) {
    return false;
  }

  @Override
  public int availablePermits() {
    return 0;
  }

  @Override
  public void release(int amount) {
    sell(amount);
  }
}
