#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <sstream>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "gps3ext.h"
#include "s3http_headers.h"
#include "s3log.h"
#include "s3reader.h"
#include "s3url_parser.h"
#include "s3utils.h"
#include "s3macros.h"

#include "s3interface.h"
using std::stringstream;

class XMLContextHolder
{
  public:
    XMLContextHolder(xmlParserCtxtPtr ctx) : context(ctx) {}
    ~XMLContextHolder()
    {
        if (context != NULL)
        {
            // TODO: confirm the order
            xmlFreeParserCtxt(context);
            xmlFreeDoc(context->myDoc);
        }

    }

  private:
    xmlParserCtxtPtr context;
};

S3Service::S3Service() {
}

S3Service::~S3Service() {
}

// S3 requires query parameters specified alphabetically.
string S3Service::getUrl(const string& prefix, const string& schema,
        const string& host, const string& bucket, const string& marker) {
    stringstream url;
    if (prefix != "") {
        url << schema << "://" << host << "/" << bucket << "?";
        if (marker != "") {
            url << "marker=" << marker << "&";
        }
        url << "prefix=" << prefix;
    } else {
        url << schema << "://" << bucket << "." << host << "?";
        if (marker != "") {
            url << "marker=" << marker;
        }
    }
    return url.str();
}


bool checkAndParseBucketXML(ListBucketResult *result,
                            xmlParserCtxtPtr xmlcontext,
                            string &marker) {
    XMLContextHolder holder(xmlcontext);

    xmlNode *rootElement = xmlDocGetRootElement(xmlcontext->myDoc);
    if (rootElement == NULL) {
        S3ERROR("Failed to parse returned xml of bucket list");
        return false;
    }

    xmlNodePtr curNode = rootElement->xmlChildrenNode;
    while (curNode != NULL) {
        if (xmlStrcmp(curNode->name, (const xmlChar *)"Message") == 0) {
            char *content = (char *)xmlNodeGetContent(curNode);
            if (content != NULL) {
                S3ERROR("Amazon S3 returns error \"%s\"", content);
                xmlFree(content);
            }
            return false;
        }

        curNode = curNode->next;
    }

    // parseBucketXML will set marker for next round.
    if (parseBucketXML(result, rootElement, marker)) {
        return true;
    }

    S3ERROR("Failed to extract key from bucket xml");
    return false;
}

// Return NULL when there is failure due to network instability or
// service unstable, so that caller could retry.
//
// Caller should delete returned object.
ListBucketResult *S3Service::ListBucket(const string &schema,
                                        const string &region,
                                        const string &bucket,
                                        const string &prefix,
                                        const S3Credential &cred) {
    stringstream host;
    host << "s3-" << region << ".amazonaws.com";
    S3DEBUG("Host url is %s", host.str().c_str());

    ListBucketResult *result = new ListBucketResult();
    CHECK_OR_DIE_MSG(result != NULL, "%s", "Failed to allocate bucket list result");

    string marker = "";
    do {                    // To get next set(up to 1000) keys.
        // S3 requires query parameters specified alphabetically.
        string url = this->getUrl(prefix, schema, host.str(), bucket, marker);

        xmlParserCtxtPtr xmlcontext = getBucketXML(region, url, prefix, cred, marker);
        if (xmlcontext == NULL) {
            S3ERROR("Failed to list bucket for %s", url.c_str());
            delete result;
            return NULL;
        }

        // parseBucketXML must not throw exception, otherwise result is leaked.
        if (! checkAndParseBucketXML(result, xmlcontext, marker)) {
            delete result;
            return NULL;
        }
    } while (! marker.empty());

    return result;
}