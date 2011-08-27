/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef STORAGELOCATION_H
#define STORAGELOCATION_H

#include <string>

namespace glite {
namespace rgma {

/**
 * Storage location for tuples.
 */
class Storage {

    public:

        /**
         * Gets a permanent named database storage object. This storage can be reused.
         *
         * @param logicalName
         *            the logical name of the storage system. If it has been used before the storage
         *            will be reused. The DN of the user is used in conjunction with the logical name as
         *            a key to the physical storage so that different users cannot share the same
         *            storage.
         *
         * @return a permanent named database storage object
         */
        Storage(const std::string & logicalName);

        /**
         * A temporary database storage object. This storage cannot be reused.
         */
        static const Storage DATABASE;

        /**
         * A temporary memory storage object. This storage cannot be reused.
         */
        static const Storage MEMORY;

    private:

        friend class PrimaryProducer;
        friend class SecondaryProducer;

        enum MD {M, D};

        Storage(MD mord);

        std::string m_logicalName;
        MD m_mord;

        bool isDatabase() const;

        bool isMemory() const;

        const std::string & getLogicalName() const;
};

}
}
#endif
