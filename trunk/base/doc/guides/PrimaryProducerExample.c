#include <stdio.h>
#include <stdlib.h>

#include "rgma/rgma.h"

#define MAXSTR 255

main(int argc, char *argv[]) {
    RGMAPrimaryProducer *pp;
    char insert[MAXSTR], predicate[MAXSTR];
    RGMAException * exception;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <userId>\n", argv[0]);
        exit(1);
    }

    pp = RGMAPrimaryProducer_create(RGMAStorageType_MEMORY, NULL,
            RGMASupportedQueries_C, &exception);
    if (exception) {
        fprintf(stderr, "Failed to create producer %s\n", exception->message);
        exit(1);
    }

    snprintf(predicate, MAXSTR, "WHERE userId = '%s'", argv[1]);

    RGMAPrimaryProducer_declareTable(pp, "default.userTable", predicate,
            3600, 3600, &exception);
    if (exception) {
        fprintf(stderr, "Failed to declare table %s\n", exception->message);
        exit(1);
    }

    snprintf(insert, MAXSTR, "INSERT INTO default.userTable (userId, aString, "
            "aReal, anInt) VALUES ('%s', 'C producer', 3.1415926, 42)", argv[1]);

    RGMAPrimaryProducer_insert(pp, insert, 0, &exception);
    if (exception) {
        fprintf(stderr, "Failed to insert %s\n", exception->message);
        exit(1);
    }

    RGMAPrimaryProducer_close(pp, &exception);
    if (exception) {
        fprintf(stderr, "Failed to close %s\n", exception->message);
        exit(1);
    }

    exit(0);
}
