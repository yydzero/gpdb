#ifndef INCLUDE_RESTFUL_SERVICE_H_
#define INCLUDE_RESTFUL_SERVICE_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include "s3http_headers.h"
using std::string;
using std::vector;
using std::map;

class Response {
   public:
    bool isSuccess() { return ! buffer.empty(); }
    vector<uint8_t>& getRawData() { return buffer; }

   private:
    vector<uint8_t> buffer;
};

class RESTfulService {
   public:
    RESTfulService();
    virtual ~RESTfulService();
    virtual Response get(const string& url, const HTTPHeaders& headers,
                 const map<string, string>& params) = 0;
};

#endif /* INCLUDE_RESTFUL_SERVICE_H_ */