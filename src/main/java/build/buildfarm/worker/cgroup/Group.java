package build.buildfarm.worker.cgroup;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.Nullable;

public class Group {
  private static final Group root = new Group(/* name=*/ null, /* parent=*/ null);
  private static final Path rootPath = Paths.get("/sys/fs/cgroup");

  private @Nullable String name;
  private @Nullable Group parent;
  private Cpu cpu;

  public static Group getRoot() {
    return root;
  }

  private Group(String name, Group parent) {
    this.name = name;
    this.parent = parent;
    cpu = new Cpu(this);
  }

  public Group getChild(String name) {
    return new Group(name, this);
  }

  public String getName() {
    return name;
  }

  public Cpu getCpu() {
    return cpu;
  }

  public String getHierarchy() {
    /* is root */
    if (parent == null) {
      return "";
    }
    /* parent is root */
    if (parent.getName() == null) {
      return getName();
    }
    /* is child of non-root parent */
    return parent.getHierarchy() + "/" + getName();
  }

  String getHierarchy(String controllerName) {
    if (parent != null) {
      return parent.getHierarchy(controllerName) + "/" + getName();
    }
    return controllerName;
  }

  Path getPath(String controllerName) {
    return rootPath.resolve(getHierarchy(controllerName));
  }

  public boolean isEmpty(String controllerName) throws IOException {
    return getProcCount(controllerName) == 0;
  }

  public int getProcCount(String controllerName) throws IOException {
    Path procs = getPath(controllerName).resolve("cgroup.procs");
    try {
      /* yes, pretty inefficient */
      return Files.readAllLines(procs).size();
    } catch (IOException e) {
      if (!Files.exists(getPath(controllerName))) {
        return 0;
      }
      throw e;
    }
  }

  public void waitUntilEmpty(String controllerName) throws IOException, InterruptedException {
    while (!isEmpty(controllerName)) {
      /* wait for event? */
      MILLISECONDS.sleep(100);
    }
  }

  void create(String controllerName) throws IOException {
    /* root already has all controllers created */
    if (parent != null) {
      parent.create(controllerName);
      Path path = getPath(controllerName);
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
    }
  }
}
