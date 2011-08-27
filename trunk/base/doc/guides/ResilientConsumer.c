#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "rgma/rgma.h"

int main(int argc, char *argv[]) {

    RGMAConsumer *consumer;
    RGMATupleSet *rs;
    RGMAException *exception;
    RGMATuple *tuple;

    const char *select = "SELECT userId, aString, aReal, anInt"
            " FROM default.userTable";

    while (1) {
        consumer = RGMAConsumer_create(select, RGMAQueryType_C, 0,
                0, 0, NULL, &exception);
        if (!exception) {
            break;
        }
        if (exception->type == RGMAExceptionType_PERMANENT) {
            fprintf(stderr, "ERROR: %s\n", exception->message);
            exit(1);
        } else {
            fprintf(stderr, "RGMATemporaryException: %s will retry "
                "in 60 seconds\n", exception->message);
            RGMA_freeException(exception);
            sleep(60);
        }
    }

    while (1) {
        rs = RGMAConsumer_pop(consumer, 2000, &exception);
        if (!exception) {
            if (rs->numTuples == 0) {
                sleep(2);
            } else {
                int i, j;
                for (i = 0; i < rs->numTuples; ++i) {
                    tuple = &rs->tuples[i];
                    printf("userId=%s, ", tuple->cols[0]);
                    printf("aString=%s, ", tuple->cols[1]);
                    printf("aReal=%s, ", tuple->cols[2]);
                    printf("anInt=%s\n", tuple->cols[3]);
                }
            }
            if (strlen(rs->warning)) {
                printf("WARNING: %s\n", rs->warning);
            }
            RGMA_freeTupleSet(rs);
        } else if (exception->type == RGMAExceptionType_PERMANENT) {
            fprintf(stderr, "ERROR: %s\n", exception->message);
            exit(1);
        } else {
            fprintf(stderr, "RGMATemporaryException: %s will retry "
                "in 60 seconds\n", exception->message);
            RGMA_freeException(exception);
            sleep(60);
        }
    }
}
