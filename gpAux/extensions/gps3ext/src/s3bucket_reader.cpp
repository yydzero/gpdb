#include <sstream>
#include <string>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "gpreader.h"
#include "gps3ext.h"
#include "reader.h"
#include "s3bucket_reader.h"
#include "s3conf.h"
#include "s3log.h"
#include "s3macros.h"
#include "s3utils.h"
#include "reader_params.h"

using std::string;
using std::stringstream;

S3BucketReader::S3BucketReader() : Reader() {
    this->keyIndex = -1;
    this->keylist = NULL;

    this->s3interface = NULL;
    this->upstreamReader = NULL;
    this->chunksize = -1;

    this->segid = -1;
    this->segnum = -1;

    this->cred.secret = s3ext_secret;
    this->cred.keyid = s3ext_accessid;

    this->needNewReader = true;
}

S3BucketReader::~S3BucketReader() {}

void S3BucketReader::setS3interface(S3Interface *s3) { this->s3interface = s3; }

void S3BucketReader::open(const ReaderParams& params) {
    this->validateURL();
    this->keylist = this->listBucketWithRetry(3);
    return;
}

BucketContent* S3BucketReader::getNextKey() {
	this->keyIndex = (this->keyIndex == (unsigned int)-1) ? this->segid : this->keyIndex + this->segnum;

	if (this->keyIndex >= this->keylist->contents.size()) {
		return NULL;
	}

	return this->keylist->contents[this->keyIndex];
}

const ReaderParams& S3BucketReader::getReaderParams(BucketContent* key) {
	ReaderParams *params = new ReaderParams();
	params->setKeyUrl(this->getKeyURL(key->getName()));
	params->setRegion(this->region);
	params->setSize(key->getSize());
	params->setChunkSize(this->chunksize);
	S3DEBUG("key: %s, size: %" PRIu64, params->getKeyUrl().c_str(), params->getSize());
	return *params;
}

uint64_t S3BucketReader::read(char *buf, uint64_t count) {
	CHECK_OR_DIE(this->upstreamReader);

	while (true) {
		if (this->needNewReader) {
			BucketContent *key = this->getNextKey();
			if (key == NULL) {
				S3DEBUG("No more files to download");
				return 0;
			}

			this->upstreamReader->open(getReaderParams(key));
			this->needNewReader = false;
		}

		uint64_t readCount = this->upstreamReader->read(buf, count);

		if (readCount != 0) {
			return readCount;
		}

		// Finished one file, continue to next
		this->upstreamReader->close();
		this->needNewReader = true;
	}
}

void S3BucketReader::close() {
	return;
}

void S3BucketReader::setUrl(string url) { this->url = url; }

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

ListBucketResult *S3BucketReader::listBucketWithRetry(int retries) {
	CHECK_OR_DIE(this->s3interface != NULL);

    while (retries--) {
        ListBucketResult *result = this->s3interface->ListBucket(
            this->schema, this->region, this->bucket, this->prefix, this->cred);
        if (result != NULL) {
            return result;
        }

        S3INFO("Can't get keylist from bucket %s, retrying ...",
               this->bucket.c_str());
    }

    S3ERROR("Failed to list bucket for URL: %s", this->url.c_str());
    CHECK_OR_DIE_MSG(false, "Failed to list bucket with retries: %s",
                     this->url.c_str());
    // return NULL;  Not needed, as CHECK_OR_DIE_MSG will return always.
}

string S3BucketReader::getKeyURL(const string &key) {
    stringstream sstr;
    sstr << this->schema << "://"
         << "s3-" << this->region << ".amazonaws.com/";
    sstr << this->bucket << "/" << key;
    return sstr.str();
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
        // for instance: s3-us-west-2.amazonaws.com
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

void S3BucketReader::validateURL() {
    this->SetSchema();
    this->SetRegion();
    this->SetBucketAndPrefix();

    bool ret = !(this->schema.empty() || this->region.empty() || this->bucket.empty());
    CHECK_OR_DIE_MSG(ret, "%s is not valid", this->url.c_str());
}