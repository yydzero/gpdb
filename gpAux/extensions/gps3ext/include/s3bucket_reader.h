#ifndef __S3_BUCKET_READER__
#define __S3_BUCKET_READER__

#include <string>

#include "s3reader.h"
#include "reader.h"

using std::string;

// S3BucketReader implements readable external table.
class S3BucketReader : public Reader {
   public:
    S3BucketReader();
    ~S3BucketReader();

    void open();
    uint64_t read(char *buf, uint64_t count);
    void close();

    void setUrl(string url);
    bool ValidateURL();

   protected:
    // Get URL for a S3 object/file.
    string getKeyURL(const string& key);

   private:
    int segid;          // segment id
    int segnum;         // total number of segments
    int chunksize;

    string url;
    string schema;
    string region;
    string bucket;
    string prefix;

    ListBucketResult* keylist;      // List of matched keys/files.
    unsigned int contentindex;      // BucketContent index of keylist->contents.


    void SetSchema();
    void SetRegion();
    void SetBucketAndPrefix();
};

#endif
