package build.buildfarm.common;

public class Inode {

  public Inode(String path, Long inode) {
    this.path = path;
    this.inode = inode;
  }

  public String getName() {
    return path;
  }

  public String path;
  public Long inode;
}
