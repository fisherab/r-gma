#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rgma.h"

#define MAX_SQL 255

int main(int argc, char *argv[]) {

    RGMAConsumer *c;
    RGMATupleSet *rs;
    RGMAException *errP;
    int numresults, endOfResults;
    char select[MAX_SQL];

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <userId>\n", argv[0]);
        return (0);
    }

    sprintf(select, "SELECT aString FROM Default.userTable WHERE userId='%s'", argv[1]);

    c = RGMAConsumer_create(select, RGMAQueryType_H, 0, 0, 0, NULL, &errP);

    if (errP) {
        fprintf(stderr, "Failed to create consumer: %s\n", errP->message);
        return (0);
    }

    numresults = 0;
    endOfResults = 0;
    do {
        sleep(5);
        rs = RGMAConsumer_pop(c, 50, &errP);
        if (errP) {
            fprintf(stderr, "Failed to pop: %s\n", errP->message);
            return (0);
        } else {
            numresults += rs->numTuples;
            endOfResults = rs->isEndOfResults;
            RGMA_freeTupleSet(rs);
        }
    } while (!endOfResults);

    if (numresults != 1) {
        fprintf(stderr, "%d tuples returned rather than 1\n", numresults);
    }

    RGMAConsumer_close(c, &errP);
    if (errP) {
        fprintf(stderr, "Failed to close: %s\n", errP->message);
    }

    return (0);
}
