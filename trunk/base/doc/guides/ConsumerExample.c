#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rgma/rgma.h"

int main(int argc, char *argv[]) {
    RGMAConsumer *consumer;
    RGMATupleSet *rs;
    RGMAException *exception;
    int endOfResults;

    const char *select = "SELECT userId, aString, aReal, anInt, RGMATimestamp "
        "FROM default.userTable";

    consumer = RGMAConsumer_create(select, RGMAQueryTypeWithInterval_C, 600,
            300, 0, NULL, &exception);
    if (exception) {
        fprintf(stderr, "Failed to create consumer. %s\n", exception->message);
        exit(1);
    }

    endOfResults = 0;
    while (!endOfResults) {
        rs = RGMAConsumer_pop(consumer, 2000, &exception);
        if (exception) {
            fprintf(stderr, "Failed to pop. %s\n", exception->message);
            exit(1);
        } else {
            if (rs->numTuples == 0) {
                sleep(2);
            } else {
                int i, j;
                for (i = 0; i < rs->numTuples; ++i) {
                    printf("userId=%s, ", rs->tuples[i].cols[0]);
                    printf("aString=%s, ", rs->tuples[i].cols[1]);
                    printf("aReal=%s, ", rs->tuples[i].cols[2]);
                    printf("anInt=%s\n", rs->tuples[i].cols[3]);
                }
            }
            endOfResults = rs->isEndOfResults;
            RGMA_freeTupleSet(rs);
        }
    }

    RGMAConsumer_close(consumer, &exception);
    exit(0);
}
