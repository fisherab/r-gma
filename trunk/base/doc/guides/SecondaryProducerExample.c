#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rgma/rgma.h"

main(int argc, char *argv[]) {

    RGMASecondaryProducer *sp;
    RGMAException * exception;
    int period;

    period = RGMA_getTerminationInterval(&exception) / 3;
    if (exception) {
        fprintf(stderr, "Failed to obtain termination interval %s\n",
                exception->message);
        exit(1);
    }

    sp = RGMASecondaryProducer_create(RGMAStorageType_DATABASE, "CExample",
            RGMASupportedQueries_CL, &exception);
    if (exception) {
        fprintf(stderr, "Failed to create producer %s\n", exception->message);
        exit(1);
    }

    RGMASecondaryProducer_declareTable(sp, "default.userTable", "",
            7200, &exception);
    if (exception) {
        fprintf(stderr, "Failed to declare table %s\n", exception->message);
        exit(1);
    }

    while (1) {
        RGMASecondaryProducer_showSignOfLife(sp, &exception);
        if (exception) {
            fprintf(stderr, "The showSignOfLife call failed %s\n",
                    exception->message);
            RGMASecondaryProducer_close(sp, &exception);
            exit(1);
        }
        sleep(period);
    }
}
