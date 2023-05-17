#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <grp.h>

int
main(int argc, char *argv[])
{
  if (argc <= 1) {
    fprintf(stderr, "usage: as-nobody [command]\n");
    return EXIT_FAILURE;
  }
  struct passwd *pw = getpwnam("nobody");
  if (pw == NULL) {
    perror("getpwnam");
    return EXIT_FAILURE;
  }
  if (setgroups(0, NULL) < 0) {
    perror("setgroups");
    return EXIT_FAILURE;
  }
  if (setregid(pw->pw_gid, pw->pw_gid) < 0) {
    perror("setregid");
    return EXIT_FAILURE;
  }
  if (setreuid(pw->pw_uid, pw->pw_uid) < 0) {
    perror("setreuid");
    return EXIT_FAILURE;
  }
  execvp(argv[1], argv + 1);
  perror("execvp");
  return EXIT_FAILURE;
}
