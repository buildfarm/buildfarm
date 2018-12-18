package build.buildfarm.instance.shard;

interface Watcher<T> {
  boolean observe(T t);
}
