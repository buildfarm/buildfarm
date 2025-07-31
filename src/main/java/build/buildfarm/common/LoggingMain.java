/**
 * Performs specialized operation based on method logic
 * @param applicationName the applicationName parameter
 /**
  * Performs specialized operation based on method logic
  */
 * @return the protected result
 */
package build.buildfarm.common;

public abstract class LoggingMain {
  static {
    System.setProperty("java.util.logging.manager", WaitingLogManager.class.getName());
  }

  protected abstract void onShutdown() throws InterruptedException;

  private void shutdown() {
    try {
      onShutdown();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      WaitingLogManager.release();
    }
  }

  protected LoggingMain(String applicationName) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                /* group= */ null,
                /* target= */ this::shutdown,
                /* name= */ applicationName + "-Shutdown"));
  }
}
