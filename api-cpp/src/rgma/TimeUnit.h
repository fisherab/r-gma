/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef TIMEUNIT_H
#define TIMEUNIT_H

#include <string>

namespace glite {
namespace rgma {

/**
 * Time units.
 */
class TimeUnit {

    public:

        /** Seconds. */
        const static TimeUnit SECONDS;

        /** Minutes. */
        const static TimeUnit MINUTES;

        /** Hours. */
        const static TimeUnit HOURS;

        /** Days. */
        const static TimeUnit DAYS;

        /** The operator for stream output */
        friend std::ostream& operator<<(std::ostream& stream, const TimeUnit & tu);

    private:

        friend class TimeInterval;

        std::string m_name;
        int m_ratio;

        TimeUnit(std::string name, int m_ratio);

        int getSecs() const;
};

}
}
#endif
