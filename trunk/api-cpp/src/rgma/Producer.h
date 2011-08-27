/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef PRODUCER_H
#define PRODUCER_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/ResourceEndpoint.h"
#include "rgma/Resource.h"

#include <string>

namespace glite {
namespace rgma {

/**
 * A Producer
 */
class Producer: public Resource {
    public:

        ~Producer();

        /**
         * Return the ResourceEndpoint to be used in the constructor of a Consumer to make a "directed query"
         *
         * @return the resourceEndpoint
         */
        ResourceEndpoint getResourceEndpoint();

    private:

        friend class PrimaryProducer;
        friend class SecondaryProducer;
        friend class OnDemandProducer;

        Producer(std::string const &servletName);

};
}
}
#endif
