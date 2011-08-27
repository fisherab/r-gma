/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RESOURCE_ENDPOINT_H
#define RESOURCE_ENDPOINT_H

#include <string>

namespace glite {
namespace rgma {
/**
 * A resource endpoint contains a URL and a resource ID.
 */
class ResourceEndpoint {
    public:

        /**
         * Creates a new ResourceEndpoint object.
         *
         * @param urlString the service URL
         * @param resourceId the resource identifier
         */
        ResourceEndpoint(const std::string & urlString, int resourceId);

        /**
         * Get the resourceId of this Endpoint
         *
         * @return the resourceId
         */
        int getResourceId() const;

        /**
         * Get a constant reference to the URL of this endpoint
         *
         * @return a constant reference to the URL
         */
        const std::string & getUrlString() const;

    private:

        friend class Resource;
        friend class Consumer;
        friend class PrimaryProducer;
        friend class SecondaryProducer;
        friend class OnDemandProducer;

        ResourceEndpoint(const std::string & servletName);
        void setResourceId(int id);

        // Data
        std::string m_url;
        int m_resourceId;
};
std::ostream& operator<<(std::ostream& stream, const ResourceEndpoint & rs);

}
}
#endif
