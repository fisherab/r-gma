/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef TUPLE_H
#define TUPLE_H

#include "rgma/RGMAPermanentException.h"
#include "rgma/RGMAType.h"

#include <vector>
#include <string>

namespace glite {
namespace rgma {

/**
 * A row of a table.
 */
class Tuple {

    public:

        /**
         * Returns the double representation of the specified column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return the double representation of the specified column.
         * Null values are returned as 0.
         *
         * @see #isNull
         */
        double getDouble(unsigned columnOffset) const throw(RGMAPermanentException);

        /**
         * Returns the float representation of the specified column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return the float representation of the specified column.
         * Null values are returned as 0.
         *
         * @see #isNull
         */
        float getFloat(unsigned columnOffset) const throw(RGMAPermanentException);

        /**
         * Returns the string representation of the specified column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return the std::string representation of the specified column.
         * Null values are returned as an empty string.
         *
         * @see #isNull
         */
        const std::string & getString(unsigned columnOffset) const throw(RGMAPermanentException);

        /**
         * Returns the integer representation of the specified column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return the int representation of the specified column.
         * Null values are returned as 0.
         *
         * @see #isNull
         */
        int getInt(unsigned columnOffset) const throw(RGMAPermanentException);

        /**
         * Returns the bool representation of the specified column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return the bool representation of the specified column.
         * Null values are returned as false.
         *
         * @see #isNull
         */
        bool getBool(unsigned columnOffset) const throw(RGMAPermanentException);

        /**
         * Returns the null status of a column.
         * @param columnOffset
         *            offset of the column within the tuple
         *
         * @return true if the column value is null and false otherwise.
         */
        bool isNull(unsigned columnOffset) const throw(RGMAPermanentException);

    private:

        friend class XMLConverter;
        friend class Consumer;
        friend class OnDemandProducer;
        friend class PrimaryProducer;
        friend class RGMAService;
        friend class Registry;
        friend class Resource;
        friend class Schema;
        friend class SecondaryProducer;

        void checkOffset(unsigned columnOffset) const throw(RGMAPermanentException);

        void addItem(const std::string & value, const bool isNull = false);

        std::vector<std::string> m_values; /* Value is not interesting if corresponding isNulls value is set */
        std::vector<bool> m_isNulls;

        static const std::string s_emptyString;

};

}
}
#endif
