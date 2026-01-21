package build.buildfarm.worker;

import build.buildfarm.common.Claim;
import build.buildfarm.common.Resource;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class Market implements Runnable, Resource {
  public static final String RESOURCE_NAME = "cpu-market";

  private final BlockingQueue<Order> orders = new LinkedBlockingQueue<>(); // buys
  private final Thread broker = new Thread(this);
  private int balance = 0;

  synchronized int balance() {
    return balance;
  }

  Order buy(int amount, Consumer<Integer> onBuy) {
    Order order = new Order(amount, onBuy);
    synchronized (this) {
      orders.add(order);
      notifyAll();
    }
    return order;
  }

  @Override
  public void run() {
    try {
      int totalBought = 0;
      for (;;) {
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
            wait();
          }
        }
      }
    } catch (InterruptedException e) {
      // ignore
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
      // checkState(amount > 0);
      sellSyncPositive(amount);
    }
  }

  synchronized void sellSyncPositive(int amount) {
    balance += amount;
    notifyAll();
  }

  // maybe we should have a composition instead of the inheritance...

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
