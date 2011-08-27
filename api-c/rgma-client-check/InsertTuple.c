#include <stdio.h>
#include <stdlib.h>

#include "rgma.h"

#define MAX_SQL 255

int main(int argc, char *argv[]) {
    RGMAPrimaryProducer *ppP;
    RGMAException *errP;
    int historyRP, latestRP;
    char insert[MAX_SQL], predicate[MAX_SQL];

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <userId>\n", argv[0]);
        return (0);
    }

    ppP = RGMAPrimaryProducer_create(RGMAStorageType_MEMORY, NULL, RGMASupportedQueries_CH, &errP);
    if (errP) {
        fprintf(stderr, "Failed to create producer: %s\n", errP->message);
        return (0);
    }

    snprintf(predicate, MAX_SQL, "WHERE userId = '%s'", argv[1]);
    historyRP = 600;
    latestRP = 600;
    RGMAPrimaryProducer_declareTable(ppP, "Default.userTable", predicate, historyRP, latestRP, &errP);

    if (errP) {
        fprintf(stderr, "Failed to declare table: %s\n", errP->message);
        return (0);
    }

    snprintf(insert, MAX_SQL,
            "INSERT INTO Default.userTable (userId, aString, aReal, anInt) \
                   VALUES ('%s', 'C producer', 3.1415926, 42)",
            argv[1]);

    RGMAPrimaryProducer_insert(ppP, insert, 0, &errP);
    if (errP) {
        fprintf(stderr, "Failed to insert: %s\n", errP->message);
        return (0);
    }

    RGMAPrimaryProducer_close(ppP, &errP);
    if (errP) {
        fprintf(stderr, "Failed to close: %s\n", errP->message);
    }
    return (0);
}
