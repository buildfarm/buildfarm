#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <grp.h>
#include <string.h>

int
main(int argc, char *argv[])
{
  int argi = 1;
  char *username = "nobody";

  if (argc <= 1) {
    fprintf(stderr, "usage: as-nobody [-u <user>] <command>\n");
    return EXIT_FAILURE;
  }
  if (strcmp(argv[1], "-u") == 0) {
    username = argv[2];
    argi = 3;
  }
  struct passwd *pw = getpwnam(username);
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
  execvp(argv[argi], argv + argi);
  perror("execvp");
  return EXIT_FAILURE;
}
