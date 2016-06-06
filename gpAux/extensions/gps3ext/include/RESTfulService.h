#ifndef INCLUDE_RESTFULSERVICE_H_
#define INCLUDE_RESTFULSERVICE_H_

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
   private:
    vector<uint8_t> buffer;
};

class RESTfulService {
   public:
    RESTfulService();
    virtual ~RESTfulService();
    virtual Response Get(const string& url, const HTTPHeaders& headers,
                 const map<string, string>& params) = 0;
};

#endif /* INCLUDE_RESTFULSERVICE_H_ */