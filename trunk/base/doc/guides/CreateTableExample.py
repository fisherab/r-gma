#!/usr/bin/env python

import sys
import rgma

vdb = "default"
create = "create table userTable (userId VARCHAR(255) NOT NULL " \
         "PRIMARY KEY, aString VARCHAR(255), aReal REAL, anInt INTEGER)"
rule = "::RW"

try:
    rgma.Schema(vdb).createTable(create, rule)
    
except rgma.RGMAException, e:
    sys.stderr.write("RGMAException " +  e.getMessage() + "\n")
    sys.exit(1)
