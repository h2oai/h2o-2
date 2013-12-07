#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

int main(void) {
    printf("Flushing page cache, dentries and inodes...\n");
    if (geteuid() != 0) {
        fprintf(stderr, "flush-cache: Not root\n");
        exit(EXIT_FAILURE);
    }
    if (system("sync") != 0) {
        fprintf(stderr, "flush-cache: sync failed (first time)\n");
        exit(EXIT_FAILURE);
    }
    if (system("sync") != 0) {
        fprintf(stderr, "flush-cache: sync failed (second time)\n");
        exit(EXIT_FAILURE);
    }
    if (system("sync") != 0) {
        fprintf(stderr, "flush-cache: sync failed (third time)\n");
        exit(EXIT_FAILURE);
    }
    FILE* f;
    f = fopen("/proc/sys/vm/drop_caches", "w");
    if (f == NULL) {
        fprintf(stderr, "flush-cache: Couldn't open /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    if (fprintf(f, "3\n") != 2) {
        fprintf(stderr, "flush-cache: Couldn't write 3 to /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }
    fclose(f);
    printf("Done flushing.\n");

    return 0;
}
