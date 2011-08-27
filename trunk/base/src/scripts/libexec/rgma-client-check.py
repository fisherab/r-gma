#!/usr/bin/env python

# rgma-client-check
#
# Check script for new R-GMA client installations. 
#
# Looks in $RGMA_HOME/libexec/rgma-client-check for test scripts installed by
# APIs. Each one consists of a 'producer' script which inserts a tuple with
# a specified ID, and a 'consumer' script which attempts to retrieve the tuple
# with a continuous+old query.
#
# Exits with status zero on success.

import os, sys
sys.path.append(os.path.join(os.environ["RGMA_HOME"], "lib", "python"))
import rgmautils

def main():
    rgma_home = os.environ["RGMA_HOME"]
    glite_location = os.environ["GLITE_LOCATION"]
    rgmautils.checkClients(rgma_home, glite_location)
  
if __name__ == "__main__": main()
