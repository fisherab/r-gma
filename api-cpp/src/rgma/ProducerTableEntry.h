/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef PRODUCERTABLEENTRY_H
#define PRODUCERTABLEENTRY_H

#include "rgma/ResourceEndpoint.h"
#include "rgma/TimeInterval.h"

#include <string>

namespace glite {
namespace rgma {
/**
 * Contains producer-table entry from the registry.
 */
class ProducerTableEntry {
    public:

        /**
         * Gets the producer's endpoint.
         *
         * @return an endpoint object.
         */
        const ResourceEndpoint & getEndpoint() const;

        /**
          * Return the predicate.
          *
          * @return predicate
          */
         const std::string & getPredicate() const;


        /**
         * Does the producer support continuous queries?
         *
         * @return true if producer supports continuous queries
         */
        bool isContinuous() const;

        /**
         * Does the producer support history queries?
         *
         * @return true if producer supports history queries
         */
        bool isHistory() const;

        /**
         * Does the producer support latest queries?
         *
         * @return true if producer supports latest queries
         */
        bool isLatest() const;

        /**
         * Is the producer a secondary producer?
         *
         * @return an endpoint object
         */
        bool isSecondary() const;

        /**
         * Does the producer support static queries?
         *
         * @return true if producer supports static queries
         */
        bool isStatic() const;

        /**
         * Returns the producer's history retention period.
         *
         * @return history retention period as a time interval
         */
        const TimeInterval & getRetentionPeriod() const;

        /** The operator for stream output */
        friend std::ostream& operator<<(std::ostream& stream, const ProducerTableEntry & pte);


    private:

        friend class Registry;

        ProducerTableEntry(const ResourceEndpoint & endpoint,  bool isSecondary,
                bool isContinuous, bool isStatic, bool isHistory, bool isLatest, const std::string & predicate, int hrpSec);

        // Data
        ResourceEndpoint m_endpoint;
        bool m_isSecondary;
        bool m_isContinuous;
        bool m_isStatic;
        bool m_isHistory;
        bool m_isLatest;
        std::string m_predicate;
        TimeInterval m_hrp;
};

}
}
#endif
