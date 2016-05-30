#include <sstream>
#include <string>

#include "gps3ext.h"
#include "s3conf.h"
#include "s3log.h"
#include "gpreader.h"
#include "s3utils.h"
#include "s3bucket_reader.h"
#include "reader.h"
#include "s3macros.h"

using std::string;
using std::stringstream;

S3BucketReader::S3BucketReader() : Reader() {
    this->contentindex = -1;
    this->keylist = NULL;
}

S3BucketReader::~S3BucketReader() {}

void S3BucketReader::open() {
	CHECK_OR_DIE_MSG(this->ValidateURL(), "%s is not valid", this->url.c_str());
    return;
}

uint64_t S3BucketReader::read(char *buf, uint64_t count) {
	return (uint64_t) 0;
}

void S3BucketReader::close() {
	return;
}

void S3BucketReader::setUrl(string url) {
	this->url = url;
}

// Set schema to 'https' or 'http'
void S3BucketReader::SetSchema() {
    size_t iend = this->url.find("://");
    if (iend == string::npos) {
        return;
    }

    this->schema = this->url.substr(0, iend);
    if (this->schema == "s3") {
        this->schema = s3ext_encryption ? "https" : "http";
    }
}

// Set AWS region, use 'external-1' if it is 'us-east-1' or not present
// http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
void S3BucketReader::SetRegion() {
    size_t ibegin =
        this->url.find("://s3") +
        strlen("://s3");  // index of character('.' or '-') after "3"
    size_t iend = this->url.find(".amazonaws.com");

    if (iend == string::npos) {
        return;
    } else if (ibegin == iend) {  // "s3.amazonaws.com"
        this->region = "external-1";
    } else {
        // ibegin + 1 is the character after "s3." or "s3-"
        // for case: s3-us-west-2.amazonaws.com
        this->region = this->url.substr(ibegin + 1, iend - (ibegin + 1));
    }

    if (this->region.compare("us-east-1") == 0) {
        this->region = "external-1";
    }
}

void S3BucketReader::SetBucketAndPrefix() {
    size_t ibegin = find_Nth(this->url, 3, "/");
    size_t iend = find_Nth(this->url, 4, "/");
    if ((iend == string::npos) || (ibegin == string::npos)) {
        return;
    }
    this->bucket = this->url.substr(ibegin + 1, iend - ibegin - 1);
    this->prefix = this->url.substr(iend + 1, this->url.length() - iend - 1);
}

bool S3BucketReader::ValidateURL() {
    this->SetSchema();
    this->SetRegion();
    this->SetBucketAndPrefix();

    return !(this->schema.empty() || this->region.empty() ||
             this->bucket.empty());
}
