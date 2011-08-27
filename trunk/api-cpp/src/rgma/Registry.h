/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef REGISTRY_H
#define REGISTRY_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/ProducerTableEntry.h"
#include "rgma/ServletConnection.h"

#include <vector>

namespace glite {
namespace rgma {
/**
 * A registry provides information about available producers.
 */
class Registry {
    public:

        /**
         * Constructs a registry object for the specified VDB.
         *
         * @param vdbName Name of VDB.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Registry(const std::string & vdbName) throw (RGMATemporaryException, RGMAPermanentException);

        /**
         * Returns a list of all producers for a table with their registered information.
         *
         * @param tableName Name of table
         *
         * @return a list of ProducerTableEntry objects, one for each producer .
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        std::vector<ProducerTableEntry> getAllProducersForTable(const std::string & tableName)
                throw(RGMATemporaryException, RGMAPermanentException);

    private:

         void clearServletConnection();

         // Data
         const std::string m_vdbName;

         ServletConnection m_connection;

};
}
}
#endif
