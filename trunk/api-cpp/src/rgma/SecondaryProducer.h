/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef SECONDARYPRODUCER_H
#define SECONDARYPRODUCER_H

#include "rgma/Storage.h"
#include "rgma/SupportedQueries.h"
#include "rgma/TimeInterval.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/Producer.h"

namespace glite {
namespace rgma {

/**
 * A client uses a secondary producer to republish or store information from
 * other producers.
 */
class SecondaryProducer: public Producer {

    public:

        /**
         * Creates a secondary producer to republish information.
         *
         * @param storage
         *            a Storage object to define the type and, if permanent, the name of the storage to be used
         * @param supportedQueries
         *            a SupportedQueries object to define what query types the producer will be able to support
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        SecondaryProducer(const Storage & storage, const SupportedQueries & supportedQueries)
                throw(RGMATemporaryException, RGMAPermanentException);

        ~SecondaryProducer() {
        }

        /**
         * Declares a table, specifying the retention period for history tuples.
         * Tuples will be removed from storage when the history retention period
         * expires.
         *
         * @param tableName The name of the table to declare.
         * @param predicate An SQL WHERE clause defining the subset of a table that
         *        this Producer will publish.  To publish to the whole table, an
         *        empty predicate can be used.
         * @param historyRetentionPeriod The retention period for history tuples
         *
         * @throws RemoteException If the service could not be contacted.
         * @throws UnknownResourceException If the producer resource could not be
         *         found
         * @throws RGMAException If the tableName is unknown.  If the predicate is
         *         invalid.  If historyRetentionPeriod is invalid.
         */
        void declareTable(const std::string & tableName, const std::string & predicate,
                const TimeInterval & historyRetentionPeriod) throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        void showSignOfLife() throw (RGMATemporaryException, RGMAPermanentException);

        /**
         * Return the resouceId to be used as the argument to subsequent static showSignOfLife calls.
         *
         * @return the resourceId
         */
        int getResourceId();

        /**
         * Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
         *
         * @param resourceId
         *            the identifier of the resource to be checked
         * @return true if resource is still alive otherwise false
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
         static bool showSignOfLife(int resourceId) throw (RGMAPermanentException, RGMATemporaryException);

    private:

        void doDeclareTable(const std::string & tableName, const std::string & predicate,
                const TimeInterval & hrp)
                throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException);

        void restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        // Data
        const Storage m_storage;
        const SupportedQueries m_supportedQueries;

        struct Table {
                std::string m_name;
                std::string m_predicate;
                TimeInterval m_hrp;

                Table(const std::string & name, const std::string & predicate, const TimeInterval & hrp);

        };
        std::vector<Table> m_tables;

        typedef std::vector<Table>::const_iterator tables_iterator;

};
}
}
#endif
