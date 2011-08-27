/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef CONSUMER_H
#define CONSUMER_H

#include "rgma/Resource.h"
#include "rgma/ResourceEndpoint.h"
#include "rgma/TupleSet.h"
#include "rgma/TimeInterval.h"
#include "rgma/QueryType.h"
#include "rgma/QueryTypeWithInterval.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"

namespace glite {
namespace rgma {
/**
 * A client uses a Consumer to retrieve data from one or more producers.
 */
class Consumer: public Resource {

    public:

        /**
         * Creates a consumer with the specified query type and where a timeout and list of producers may also be specified.
         * For a continuous query, only tuples published after the consumer has been created will be returned. All tuples
         * are included for other query types. The query will terminate after the specified time interval and will make use
         * of the specified list of producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param timeout
         *            time interval after which the query will be aborted.
         * @param producers
         *            list of producers to contact.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout, std::vector<
                ResourceEndpoint> producers) throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and where a timeout may also be specified. For a continuous
         * query, only tuples published after the consumer has been created will be returned. All tuples are included for
         * other query types. The query will terminate after the specified time interval and the mediator will be used to
         * find suitable producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param timeout
         *            time interval after which the query will be aborted. May be null to have no timeout.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout)
                throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and where a list of producers may also be specified. For a
         * continuous query, only tuples published after the consumer has been created will be returned. All tuples are
         * included for other query types. The query will make use of the specified list of producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param producers
         *            list of producers to contact.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryType & queryType, std::vector<ResourceEndpoint> producers)
                throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type. For a continuous query, only tuples published after the
         * consumer has been created will be returned. All tuples are included for other query types. The mediator will be
         * used to find suitable producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryType & queryType) throw (RGMAPermanentException,
                RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and query interval and where a timeout and list of producers may
         * also be specified. The query will terminate after the specified time interval and will make use of the specified
         * list of producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param queryInterval
         *            the time interval is subtracted from the current time to give a time in the past. The query result
         *            will then use data from tuples published after this time.
         * @param timeout
         *            time interval after which the query will be aborted.
         * @param producers
         *            list of producers to contact.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
                const TimeInterval & queryInterval, const TimeInterval & timeout,
                std::vector<ResourceEndpoint> producers) throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and query interval and where a timeout may also be specified.
         * The query will terminate after the specified time interval and the mediator will be used to find suitable
         * producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param queryInterval
         *            the time interval is subtracted from the current time to give a time in the past. The query result
         *            will then use data from tuples published after this time.
         * @param timeout
         *            time interval after which the query will be aborted.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
                const TimeInterval & queryInterval, const TimeInterval & timeout) throw (RGMAPermanentException,
                RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and query interval and where a list of producers may also be
         * specified. The query will make use of the specified list of producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param queryInterval
         *            the time interval is subtracted from the current time to give a time in the past. The query result
         *            will then use data from tuples published after this time.
         * @param producers
         *            list of producers to contact.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
                const TimeInterval & queryInterval, std::vector<ResourceEndpoint> producers)
                throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Creates a consumer with the specified query type and query interval. The mediator will be used to find suitable
         * producers.
         *
         * @param query
         *            a SQL select statement
         * @param queryType
         *            the type of the query
         * @param queryInterval
         *            the time interval is subtracted from the current time to give a time in the past. The query result
         *            will then use data from tuples published after this time.
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
                Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
                        const TimeInterval & queryInterval) throw (RGMAPermanentException, RGMATemporaryException);

        ~Consumer();

        /**
         * Aborts the current query.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        void abort() throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Determines if the last query has aborted.
         *
         * @return True if the query was aborted and didn't stop of its own accord.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         *
         */
        bool hasAborted() throw(RGMATemporaryException, RGMAPermanentException);
        /**
         * Retrieves at most <code>maxCount</code> tuples from the consumer that it
         * has received from producers.
         *
         * @param maxCount The maximum number of tuples to retrieve.
         * @param results a resultSet object to be populated with the results of the pop action
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        void pop(int maxCount, TupleSet& results) throw(RGMATemporaryException, RGMAPermanentException);

    private:

        Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout,
                const QueryTypeWithInterval & queryTypeWithInterval, const std::vector<ResourceEndpoint> & endpoints,
                const TimeInterval & queryInterval);

        void doAbort() throw(RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        bool doHasAborted() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        void doPop(int maxCount, TupleSet& results) throw(RGMAPermanentException, RGMATemporaryException,
                UnknownResourceException);

        int create() throw(RGMAPermanentException, RGMATemporaryException);

        void restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException);

        void checkProducerList() const throw (RGMAPermanentException);

        // Data
        const std::string m_query;
        const QueryType m_queryType;
        const TimeInterval m_timeout;
        const QueryTypeWithInterval m_queryTypeWithInterval;
        const std::vector<ResourceEndpoint> m_endpoints;
        const TimeInterval m_queryInterval;
        bool m_eof;

        typedef std::vector<ResourceEndpoint>::const_iterator ep_iterator;
};
}
}
#endif
