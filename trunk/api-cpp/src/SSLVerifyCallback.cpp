/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

//#include "rgma/APILogging.hh"
#include "rgma/SSLContext.h"
#include "rgma/SSLSigningPolicyProperties.h"
#include "rgma/SSLUtils.h"
#include "rgma/SSLVerifyCallback.h"
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <openssl/asn1_mac.h>
#include <fnmatch.h>
#include <unistd.h>
#include <iostream>

namespace glite {
namespace rgma {

int SSLVerifyCallback::main(int ok, X509_STORE_CTX *X509StoreContext) {
    X509* currentCert = X509_STORE_CTX_get_current_cert(X509StoreContext);

    if (ok) {
        // Potentially a good certificate.  Carry out some more checks
        if (SSLUtils::isAProxyCertificate(currentCert)) {
            if (!proxyCertificateOk(currentCert, X509StoreContext)) {
                ok = 0;
            }
        } else {
            if (!CRLCheckOk(currentCert, X509StoreContext)) {
                // Checked if the certificate has been revoked.  Proxy certificates cannot
                // be revoked so they are not checked.
                ok = 0;
            } else if (!signingPolicyCheckOk(currentCert, X509StoreContext)) {
                ok = 0;
            }
        }
    }
    return ok;
}

bool SSLVerifyCallback::CRLCheckOk(X509* certificate, X509_STORE_CTX *X509StoreContext) {
    // We only need to check the CRLs because OpenSSL 0.9.6 doesn't do this
    // (although 0.9.7 does).  This code can be removed once migration to
    // 0.9.7 is complete.
    bool ok = true;
    X509_OBJECT X509Object;
    if (X509_STORE_get_by_subject(X509StoreContext, X509_LU_CRL, X509_get_issuer_name(certificate), &X509Object)) {
        X509_CRL* crl = X509Object.data.crl;
        ok
                = (CRLValid(certificate, crl, X509StoreContext) && CertificateNotRevoked(certificate, crl,
                        X509StoreContext));
        X509_OBJECT_free_contents(&X509Object);
    }
    return ok;
}

bool SSLVerifyCallback::CRLValid(X509* certificate, X509_CRL* crl, X509_STORE_CTX *X509StoreContext) {
    X509_CRL_INFO* crlInfo = crl->crl;
    // Check that the CRL is signed by the certificate issuer
    X509* issuer;
    if (X509StoreContext->get_issuer(&issuer, X509StoreContext, certificate) <= 0) {
        //cat.info("Could not obtain issuer certificate");
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    EVP_PKEY* issuerKey = X509_get_pubkey(issuer);
    X509_free(issuer);
    issuer = 0;

    if (!issuerKey) {
        //cat.info("Could not obtain public key for certificate issuer");
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    int crlVerify = X509_CRL_verify(crl, issuerKey);
    EVP_PKEY_free(issuerKey);
    issuerKey = 0;
    if (crlVerify <= 0) {
        //cat.info(crlVerify ? "Error verifying CRL" : "CRL verification failure");
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_SIGNATURE_FAILURE);
        return false;
    }

    // Check CRL validity
    int timeComparison = X509_cmp_current_time(crlInfo->lastUpdate);
    if (!timeComparison) {
        //cat.info("Invalid CRL last update field");
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD);
        return false;
    } else if (timeComparison > 0) {
        //cat.info("CRL not yet valid");
        X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_NOT_YET_VALID);
        return false;
    }

    if (crlInfo->nextUpdate) {
        timeComparison = X509_cmp_current_time(crlInfo->nextUpdate);
        if (!timeComparison) {
            //cat.info("Invalid CRL next update field");
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD);
            return false;
        } else if (timeComparison < 0) {
            //cat.info("CRL has expired");
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CRL_HAS_EXPIRED);
        }
    }

    //cat.debug("CRL is valid");
    return true;
}

bool SSLVerifyCallback::CertificateNotRevoked(X509* certificate, X509_CRL* crl, X509_STORE_CTX *X509StoreContext) {
    X509_CRL_INFO* crlInfo = crl->crl;
    int certificatesInCRL = sk_X509_REVOKED_num(crlInfo->revoked);
    for (int certificateIndex = 0; certificateIndex < certificatesInCRL; certificateIndex++) {
        X509_REVOKED* revoked = (X509_REVOKED *) sk_X509_REVOKED_value(crlInfo->revoked,
                certificateIndex);
        if (!ASN1_INTEGER_cmp(revoked->serialNumber, X509_get_serialNumber(certificate))) {
            // This certificate has been revoked
            //cat.info("This certificate, s/n 0x%X, has been revoked",
            //     ASN1_INTEGER_get(revoked->serialNumber));
            //X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_CERT_REVOKED);
            return false;
        }
    }

    //cat.debug("Certificate has not been revoked");
    return true;
}

bool SSLVerifyCallback::proxyCertificateOk(X509* certificate, X509_STORE_CTX *X509StoreContext) {
    bool ok = true;

    // Check that the user who issued the certificate signed the proxy
    char *subject = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
    char *issuer = X509_NAME_oneline(X509_get_issuer_name(certificate), NULL, 0);
    const char* proxyString = strstr(subject, "/CN=");
    if (proxyString) {
        proxyString += strlen("/CN");
        proxyString = strstr(proxyString, "/CN=");
        int length = (proxyString - subject) / sizeof(char);
        if (strncmp(subject, issuer, length)) {
            //cat.info("Proxy certificate signed by someone other than owner");
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_SUBJECT_ISSUER_MISMATCH);
            ok = false;
        }
    }

    // Check that the proxy is at the top of the chain.  For delegation etc.,
    // the proxy might not be at the top of the chain but RGMA does not support
    // this at present

    if (ok) {
        int depth = X509_STORE_CTX_get_error_depth(X509StoreContext);
        if (depth != 0) {
            //cat.info("Proxy certificate is not at the end of the chain");
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_APPLICATION_VERIFICATION);
            ok = false;
        }
    }

    OPENSSL_free(subject);
    subject = 0;
    OPENSSL_free(issuer);
    issuer = 0;

    return ok;
}

bool SSLVerifyCallback::signingPolicyCheckOk(X509* certificate, X509_STORE_CTX *X509StoreContext) {
    bool ok = true;

    X509_NAME* issuerName = X509_get_issuer_name(certificate);
    X509_NAME* subjectName = X509_get_subject_name(certificate);
    // Don't need to check self-signed certificates
    if (X509_NAME_cmp(subjectName, issuerName)) {
        try {
            SSLSigningPolicyProperties policy(certificate, SSLContext::getSslCAFiles());
            if (policy.present("access_id_CA") && policy.present("pos_rights") && policy.present("cond_subjects")) {

                char* issuer = X509_NAME_oneline(issuerName, NULL, 0);
                char* subject = X509_NAME_oneline(subjectName, NULL, 0);

                // Check access id matches issuer (the e-mail hack in here
                // gets around the fact that there are two alternative spellings
                // of the "Email/emailAddress" keyword).

                if (policy.getProperty("access_id_CA") != issuer) {

                    const char *p, *s, *t;
                    p = policy.getProperty("access_id_CA").c_str();

                    if ((((s = strstr(p, "/Email=")) != NULL) && ((t = strstr(issuer, "/emailAddress=")) != NULL) &&
                    // match 1st half
                            (strncmp(p, issuer, (s - p)) == 0) &&
                    // match 2nd half
                            (strcmp((s + 7), (t + 14)) == 0)) || (((s = strstr(p, "/emailAddress=")) != NULL) && ((t
                            = strstr(issuer, "/Email=")) != NULL) &&
                    // match 1st half
                            (strncmp(p, issuer, (s - p)) == 0) &&
                    // match 2nd half
                            (strcmp((s + 14), (t + 7)) == 0))) {

                        ok = true; // so far, so good
                    } else {
                        std::cerr << "The issuer is not the same as the policy access_id_CA field" << "\n";
                        ok = false;
                    }

                }

                if (ok) {
                    // Check that the certificate has CA signing rights
                    if (policy.getProperty("pos_rights") != "CA:sign") {
                        std::cerr << "The CA certificate is no longer used to sign certificates" << "\n";
                        ok = false;
                    }
                }

                if (ok) {
                    ok = false;
                    // Check that the subject is one which the CA can sign
                    std::vector<std::string> subjects = policy.getListProperty("cond_subjects");
                    std::vector<std::string>::iterator it = subjects.begin();
                    while (it != subjects.end()) {
                        const char* thisMatch = (*it).c_str();
                        if (!fnmatch(thisMatch, subject, FNM_NOESCAPE)) {
                            ok = true;
                            break;
                        }
                        ++it;
                    }
                    if (!ok) {
                        std::cerr << "Subject is not one that the signing policy allows the issuer to sign" << "\n";
                    }
                }

                OPENSSL_free(subject);
                subject = 0;
                OPENSSL_free(issuer);
                issuer = 0;
            } else {
                std::cerr << "Policy doesn't match certificate" << "\n";
                ok = false;
            }
        } catch (std::exception& e) {
            std::cerr << "Cannot load signing policy file " << e.what() << std::endl;
            ok = false;
        } catch (...) {
            std::cerr << "Cannot load signing policy file - ellipsis was caught" << std::endl;
            ok = false;
        }
        // Only one error code for all signing policy failures
        if (!ok)
            X509_STORE_CTX_set_error(X509StoreContext, X509_V_ERR_APPLICATION_VERIFICATION);
    }
    return ok;
}

}
}
