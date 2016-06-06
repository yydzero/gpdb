#ifndef INCLUDE_RESTFUL_H_
#define INCLUDE_RESTFUL_H_

#include <stdint.h>
#include <string>
#include <vector>
#include "s3http_headers.h"
using std::string;
using std::vector;

class Response {
   public:
   private:
    vector<uint8_t> buffer;
};

class RESTful {
   public:
    RESTful();
    virtual ~RESTful();
    bool Init(const string& url, const HTTPHeaders& headers);
    Response Get();
};

#endif /* INCLUDE_RESTFUL_H_ */
