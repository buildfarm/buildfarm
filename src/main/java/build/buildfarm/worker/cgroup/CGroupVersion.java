package build.buildfarm.worker.cgroup;

public enum CGroupVersion {
  NONE, /* CGroups not detected at all */
  CGROUPS_V1,
  CGROUPS_V2,
}
