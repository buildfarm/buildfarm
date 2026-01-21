package build.buildfarm.worker;

import static com.google.common.base.Preconditions.checkState;

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
    checkState(available > 0);
    int sharesToBuy = Math.min(available, balance);
    balance -= sharesToBuy;
    onBuy.accept(sharesToBuy);
    return sharesToBuy;
  }

  synchronized int balance() {
    return balance;
  }
}
