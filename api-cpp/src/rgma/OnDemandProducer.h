/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef ONDEMANDPRODUCER_H
#define ONDEMANDPRODUCER_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/Producer.h"

#include <string>

namespace glite {
namespace rgma {

/**
 * A client uses an OnDemandProducer to publish data into R-GMA when the cost
 * of creating each message is high. This producer only generates
 * messages when there is a specific query from a Consumer.
 */
class OnDemandProducer: public Producer {
    public:

        /**
         * Creates an on-demand producer.
         *
         * @param hostName
         *              the host name of the system that will respond to queries
         * @param port
         *              the port on the specified host that will respond to queries
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        OnDemandProducer(const std::string & hostName, int port) throw(RGMATemporaryException, RGMAPermanentException);

        ~OnDemandProducer();

        /**
         * Declares a static table into which this Producer can publish.  A subset
         * of a table can be declared using a predicate.  A static table has no
         * MeasurementDate/Time associated with each tuple.
         *
         * @param tableName The name of the table to publish into.
         * @param predicate An SQL WHERE clause defining the subset of a table that
         *        this Producer will publish.  To publish to the whole table, an
         *        empty predicate can be defined using "".
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void declareTable(const std::string & tableName, const std::string & predicate) throw(RGMATemporaryException,
                RGMAPermanentException);

    private:

        void doDeclareTable(const std::string & tableName, const std::string & predicate) throw(RGMATemporaryException,
                RGMAPermanentException, UnknownResourceException);

        void restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        // Data
        const std::string m_hostName;
        const int m_port;

        struct Table {
                std::string m_name;
                std::string m_predicate;

                Table(const std::string & name, const std::string & predicate);

        };
        std::vector<Table> m_tables;

        typedef std::vector<Table>::const_iterator tables_iterator;

};
}
}
#endif
