package persistent.common.processes;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a process, giving it a (possible different) working directory and environment variables.
 * Redirects stderr to a file under its working dir using a random uuid, i.e. "{{randomUUID()}}.stderr"
 * Exposes its stdin as OutputStream and stdout as InputStream.
 *
 * Constructor immediately starts a process and checks isAlive() right after.
 */
public class ProcessWrapper implements Closeable {

  private static Logger logger = Logger.getLogger(ProcessWrapper.class.getName());

  private final Process process;

  private final Path workRoot;

  private final ImmutableList<String> args;

  private final Path errorFile;

  private final UUID uuid;

  public ProcessWrapper(Path workDir, ImmutableList<String> args) throws IOException {
    this(workDir, args, new HashMap<>());
  }

  public ProcessWrapper(
      Path workDir, ImmutableList<String> args, Map<String, String> env
  ) throws IOException {
    this.args = checkNotNull(args);
    this.workRoot = checkNotNull(workDir);
    Preconditions.checkArgument(
        Files.isDirectory(workDir),
        "Process workDir must be a directory, got: " + workDir
    );
    this.uuid = UUID.randomUUID();

    this.errorFile = this.workRoot.resolve(this.uuid + ".stderr");

    logger.log(Level.SEVERE, "Starting Process:");
    logger.log(Level.SEVERE, "\tcmd: " + this.args);
    logger.log(Level.SEVERE, "\tdir: " + this.workRoot);
    logger.log(Level.SEVERE, "\tenv: " + ImmutableMap.copyOf(env));
    logger.log(Level.SEVERE, "\tenv: " + errorFile);

    ProcessBuilder pb = new ProcessBuilder()
        .command(this.args)
        .directory(this.workRoot.toFile())
        .redirectError(ProcessBuilder.Redirect.to(this.errorFile.toFile()));

    pb.environment().putAll(env);

    try {
      this.process = pb.start();
    } catch (IOException e) {
      String msg = "Failed to start process: " + e;
      logger.log(Level.SEVERE, msg);
      e.printStackTrace();
      throw new IOException(msg, e);
    }

    if (!this.process.isAlive()) {
      int exitVal = this.process.exitValue();
      String msg = "Process instantly terminated with: " + exitVal + "; " + getErrorString();
      throw new IOException(msg);
    }
  }

  public ImmutableList<String> getInitialArgs() {
    return this.args;
  }

  public Path getWorkRoot() {
    return this.workRoot;
  }

  public OutputStream getStdIn() {
    return this.process.getOutputStream();
  }

  public InputStream getStdOut() {
    return this.process.getInputStream();
  }

  public String getErrorString() throws IOException {
    if (Files.exists(this.errorFile)) {
      return new String(Files.readAllBytes(this.errorFile));
    } else {
      return "[No errorFile...]";
    }
  }

  public String flushErrorString() throws IOException {
    String contents = getErrorString();
    Files.write(this.errorFile, "".getBytes(), WRITE, TRUNCATE_EXISTING);
    return contents;
  }

  public boolean isAlive() {
    return this.process.isAlive();
  }

  public int exitValue() {
    return this.process.exitValue();
  }

  public int waitFor() throws InterruptedException {
    return this.process.waitFor();
  }

  public void destroy() {
    this.process.destroyForcibly();
  }

  @Override
  public void close() throws IOException {
    this.destroy();
  }
}
