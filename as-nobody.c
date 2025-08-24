/*
 * Copyright 2021-2025 The Buildfarm Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <grp.h>
#include <string.h>
#include <errno.h>

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
  // per getpwnam, this needs to be reset before checking error
  errno = 0;
  struct passwd *pw = getpwnam(username);
  if (pw == NULL) {
    if (errno == 0) {
      // also listed in documentation as a possible error status return
      errno = ENOENT;
    }
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
