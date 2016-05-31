#ifndef __S3_BUCKET_READER__
#define __S3_BUCKET_READER__

#include <string>

#include "reader.h"
#include "s3interface.h"
#include "s3reader.h"

using std::string;

// S3BucketReader read multiple files in a bucket.
class S3BucketReader : public Reader {
   public:
    S3BucketReader();
    ~S3BucketReader();

    void open();
    uint64_t read(char *buf, uint64_t count);
    void close();

    void setS3interface(S3Interface *s3);
    void setUpstreamReader(Reader *reader);

    void setUrl(string url);
    void validateURL();
    ListBucketResult *listBucketWithRetry(int retries);

   protected:
    // Get URL for a S3 object/file.
    string getKeyURL(const string &key);
    bool getNextDownloader();

   private:
    int segid;   // segment id
    int segnum;  // total number of segments
    int chunksize;

    string url;
    string schema;
    string region;
    string bucket;
    string prefix;

    S3Credential cred;
    S3Interface *s3interface;

    // upstreamReader is where we get data from.
    Reader *upstreamReader;

    ListBucketResult *keylist;  // List of matched keys/files.
    unsigned int contentindex;  // BucketContent index of keylist->contents.

    void SetSchema();
    void SetRegion();
    void SetBucketAndPrefix();
};

#endif
