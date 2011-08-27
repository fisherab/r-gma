#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "rgma/rgma.h"

#define MAXSTR 255

int main(int argc, char *argv[]) {
    RGMAPrimaryProducer *pp;
    int hrpSec = 50* 60 ;
    int lrpSec = 25* 60 ;
    char insert[MAXSTR], predicate[MAXSTR];
    int data = 0;
    RGMAException *exception;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <userId>\n", argv[0]);
        exit(1);
    }

    snprintf(predicate, MAXSTR, "WHERE userId = '%s'", argv[1]);

    while (1) {
        pp = RGMAPrimaryProducer_create(RGMAStorageType_MEMORY, NULL, RGMASupportedQueries_C, &exception);
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
        RGMAPrimaryProducer_declareTable(pp, "default.userTable", predicate, hrpSec, lrpSec, &exception);
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
        snprintf(insert, MAXSTR, "INSERT INTO default.userTable (userId, aString, "
            "aReal, anInt) VALUES ('%s', 'resilient C producer', 0.0, %d)", argv[1], data);
        RGMAPrimaryProducer_insert(pp, insert, 0, &exception);
        if (!exception) {
            printf("%s\n", insert);
            data++;
            sleep(30);
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
