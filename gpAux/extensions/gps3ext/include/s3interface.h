#ifndef INCLUDE_S3INTERFACE_H_
#define INCLUDE_S3INTERFACE_H_

#include "s3reader.h"

class S3Interface {
   public:
    virtual ~S3Interface() {}

    // It is caller's responsibility to free returned memory.
    virtual ListBucketResult* ListBucket(const string& schema,
                                         const string& region,
                                         const string& bucket,
                                         const string& prefix,
                                         const S3Credential& cred) = 0;
};

class S3Service : public S3Interface {
   public:
    S3Service();
    virtual ~S3Service();
    ListBucketResult* ListBucket(const string& schema, const string& region,
                                 const string& bucket, const string& prefix,
                                 const S3Credential& cred);

    void SetRESTfulService(RESTfulService* service) { this->service = service; }

   private:
    string getUrl(const string& prefix, const string& schema,
                  const string& host, const string& bucket,
                  const string& marker);
    bool parseBucketXML(ListBucketResult* result, xmlNode* root_element,
                        string& marker);
    xmlParserCtxtPtr getBucketXML(const string& region, const string& url,
                                  const string& prefix,
                                  const S3Credential& cred,
                                  const string& marker);
    bool checkAndParseBucketXML(ListBucketResult* result,
                                xmlParserCtxtPtr xmlcontext, string& marker);

    RESTfulService* service;
};

#endif /* INCLUDE_S3INTERFACE_H_ */
