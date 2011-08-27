/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

#include "rgma/SSLCertVerifyCallback.h"
#include "rgma/SSLUtils.h"
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

namespace glite
{
    namespace rgma
    {

        int SSLCertVerifyCallback::main(X509_STORE_CTX* X509StoreContext,
            void* argument)
        {
            X509StoreContext->check_issued = &SSLCertVerifyCallback::checkIssued;
            return X509_verify_cert(X509StoreContext);
        }

        int SSLCertVerifyCallback::checkIssued(X509_STORE_CTX* X509StoreContext,
            X509* certificate,
            X509* candidateIssuerCertificate)
        {
            int issuedStatus = X509_check_issued(candidateIssuerCertificate, certificate);
            switch (issuedStatus)
            {
                case X509_V_OK:
                    // Found match
                    return 1;

                case X509_V_ERR_KEYUSAGE_NO_CERTSIGN:
                    // Found match, except that the candidateIssuerCertificate isn't allowed
                    // to sign certificates.  Ignore this if the issued certificate is a proxy.
                    if (SSLUtils::isAProxyCertificate(certificate)) return 1;
                    break;

                default:
                    break;
            }

            // Set up error details for issuer errors if requested.
#if OPENSSL_VERSION_NUMBER < 0x00908000
			if (X509StoreContext->flags & X509_V_FLAG_CB_ISSUER_CHECK){
#else
			if (X509StoreContext->param->flags & X509_V_FLAG_CB_ISSUER_CHECK){
#endif
                X509StoreContext->error = issuedStatus;
                X509StoreContext->current_cert = certificate;
                X509StoreContext->current_issuer = candidateIssuerCertificate;
                if (X509StoreContext->verify_cb) return X509StoreContext->verify_cb(0, X509StoreContext);
            }
            return 0;
        }

    }
}


#if RGMA_OPENSSL_CERT_VERIFY_HACK == 1
extern "C" int SSLCertVerifyCallbackFunction(X509_STORE_CTX* X509StoreContext,
void * argument)
{
    return edg::info::SSLCertVerifyCallback::main(X509StoreContext, argument);
}
#endif
