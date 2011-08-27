/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef PRIMARYPRODUCER_H
#define PRIMARYPRODUCER_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/TimeInterval.h"
#include "rgma/Storage.h"
#include "rgma/SupportedQueries.h"
#include "rgma/Producer.h"

#include <vector>
#include <string>

namespace glite {
namespace rgma {

/**
 * A client uses a PrimaryProducer to publish information into R-GMA.
 */
class PrimaryProducer: public Producer {

    public:

        /**
         * Creates a primary producer that uses the specified data storage and supported queries.
         *
         * @param storage
         *            a Storage object to define the type and, if permanent, the name of the storage to be used
         * @param supportedQueries
         *            a SupportedQueries object to define what query types the producer will be able to support.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        PrimaryProducer(const Storage & storage, const SupportedQueries & supportedQueries)
                throw(RGMATemporaryException, RGMAPermanentException);

        ~PrimaryProducer();

        /**
         * Declares a table, specifying the retention period for history and latest
         * tuples.  The latestRetentionPeriod specifies the time for which a
         * latest tuple is valid.  After this time, the tuple is removed.  Tuples
         * will be removed from storage when the history retention period expires.
         *
         * @param tableName The name of the table to declare.
         * @param predicate An SQL WHERE clause defining the subset of a table that
         *        this Producer will publish.  To publish to the whole table, an
         *        empty predicate can be used.
         * @param historyRetentionPeriod The retention period for history tuples
         * @param latestRetentionPeriod The retention period for latest tuples
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */

        void declareTable(const std::string & tableName, const std::string & predicate,
                const TimeInterval & historyRetentionPeriod, const TimeInterval & latestRetentionPeriod)
                throw(RGMATemporaryException, RGMAPermanentException);
        /**
         * Publishes data by inserting a tuple into a table, both specified by the
         * SQL INSERT statement.
         *
         * @param insertStatement An SQL INSERT statement providing the data to
         *        publish and the table into which to put it.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException       *
         */
        void insert(const std::string & insertStatement) throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Publishes data by inserting a tuple into a table, both specified by the
         * SQL INSERT statement.
         *
         * @param insertStatement An SQL INSERT statement providing the data to
         *        publish and the table into which to put it.
         * @param latestRetentionPeriod Latest retention period for this tuple (overrides
         *        LRP defined for table).
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void insert(const std::string & insertStatement, const TimeInterval & latestRetentionPeriod)
                throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Publishes using a list of SQL INSERT statements.
         *
         * @param insertStatements A list of SQL INSERT statements.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void insert(const std::vector<std::string> & insertStatements) throw(RGMATemporaryException,
                RGMAPermanentException);

        /**
         * Publishes using a list of SQL INSERT statements.
         *
         * @param insertStatements A list of SQL INSERT statements.
         * @param latestRetentionPeriod the retention period for latest queries
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void insert(const std::vector<std::string> & insertStatements, const TimeInterval & latestRetentionPeriod)
                throw(RGMATemporaryException, RGMAPermanentException);

    private:

        void doDeclareTable(const std::string & tableName, const std::string & predicate,
                const TimeInterval & historyRetentionPeriod, const TimeInterval & latestRetentionPeriod)
                throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException);

        void doInsert(const std::vector<std::string> & insertStatements) throw(RGMATemporaryException,
                RGMAPermanentException, UnknownResourceException);

        void doInsert(const std::vector<std::string> & insertStatements, const TimeInterval & lrp)
                throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException);

        void restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        // Data
        const Storage m_storage;
        const SupportedQueries m_supportedQueries;

        struct Table {
                std::string m_name;
                std::string m_predicate;
                TimeInterval m_hrp;
                TimeInterval m_lrp;

                Table(const std::string & name, const std::string & predicate,
                        const TimeInterval & historyRetentionPeriod, const TimeInterval & latestRetentionPeriod);

        };
        std::vector<Table> m_tables;

        typedef std::vector<Table>::const_iterator tables_iterator;

};

}
}
#endif
