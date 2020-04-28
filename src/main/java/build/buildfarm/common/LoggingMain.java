package build.buildfarm.common;

public abstract class LoggingMain {
  static {
    System.setProperty("java.util.logging.manager", WaitingLogManager.class.getName());
  }

  protected abstract void onShutdown() throws InterruptedException;

  class ShutdownThread extends Thread {
    ShutdownThread(String applicationName) {
      super(null, null, applicationName + "-Shutdown", 0);
    }

    @Override
    public void run() {
      try {
        LoggingMain.this.onShutdown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        WaitingLogManager.release();
      }
    }
  }

  protected LoggingMain(String applicationName) {
    Runtime.getRuntime().addShutdownHook(new ShutdownThread(applicationName));
  }
}
