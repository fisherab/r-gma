/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_TIMEINTERVAL_H
#define RGMA_TIMEINTERVAL_H

#include <string>
#include "rgma/RGMAPermanentException.h"
#include "rgma/TimeUnit.h"

namespace glite {
namespace rgma {

/**
 * Encapsulates a time value and the units being used.
 */
class TimeInterval {

    public:
        /**
         *  A class that can represent a time interval
         *
         * @param value a long value
         * @param units The time units: SECONDS, MINUTES, HOURS, DAYS
         *
         */
        TimeInterval(int value, const TimeUnit & units = TimeUnit::SECONDS) throw(RGMAPermanentException);

        /**
         * Gets the length of the time interval in the specified units.
         *
         * @param units The time units: SECONDS, MINUTES, HOURS, DAYS
         *
         * @return The time interval in the given units.
         */
        int getValueAs(const TimeUnit & units) const throw(RGMAPermanentException);

        /** The operator for stream output */
        friend std::ostream& operator<<(std::ostream& stream, const TimeInterval & ti);

    private:

        friend class Consumer;

        /**
         * A "null" TimeInterval
         */
        static const TimeInterval NONE;

        TimeInterval();

        bool operator==(const TimeInterval & ti) const;

        // Data
        int m_value;
        TimeUnit m_units;

};
}
}
#endif
