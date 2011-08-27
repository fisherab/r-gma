/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RESOURCE_H
#define RESOURCE_H

#include "rgma/ResourceEndpoint.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/ServletConnection.h"

namespace glite {
namespace rgma {

/**
 * A producer or consumer.
 */
class Resource {

    public:

        virtual ~Resource();

        /**
         * Closes and destroys the resource. The resource will no
         * longer be available to any component. It is not an error
         * if the resource does not exist.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void destroy() throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Closes the resource. The resource will no longer be available through this API and
         * will be destroyed once it has finished interacting with other components. It is not
         * an error if the resource does not exist.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void close() throw(RGMATemporaryException, RGMAPermanentException);

    private:

        friend class ResourceEndpoint;
        friend class Consumer;
        friend class PrimaryProducer;
        friend class SecondaryProducer;
        friend class OnDemandProducer;
        friend class Producer;

        Resource(std::string const &servletName);

        void checkOK(TupleSet& ts) const throw (RGMAPermanentException);

        void clearServletConnection();

        // Data - the endpoint must be initialised before the servlet connection
        ResourceEndpoint m_endPoint;
        ServletConnection m_connection;
        bool m_dead;

};
}
}
#endif
