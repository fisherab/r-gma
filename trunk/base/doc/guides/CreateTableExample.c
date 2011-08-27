#include <stdio.h>
#include <stdlib.h>

#include "rgma/rgma.h"

int main(int argc, char *argv[]) {
    char *vdb = "default";
    char *create = "create table userTable (userId VARCHAR(255) NOT NULL \
           PRIMARY KEY, aString VARCHAR(255), aReal REAL, anInt INTEGER)";
    char *rules[] = { "::RW" };
    int numRules = 1;
    RGMAException *exception;

    RGMASchema_createTable(vdb, create, numRules, rules, &exception);
    if (exception) {
        fprintf(stderr, "Failed to create table %s\n", exception->message);
        RGMA_freeException(exception);
        exit(1);
    }
    exit(0);
}
