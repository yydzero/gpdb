#include "s3interface.cpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

class S3ServiceTest : public testing::Test {
   protected:
    // Remember that SetUp() is run immediately before a test starts.
    virtual void SetUp() {
        s3ext_logtype = STDERR_LOG;
        s3ext_loglevel = EXT_INFO;

        s3service = new S3Service();
        schema = "https";
    }

    // TearDown() is invoked immediately after a test finishes.
    virtual void TearDown() {
        delete s3service;
    }

    S3Service* s3service;
    S3Credential cred;
    string schema;
    string region;
    string bucket;
    string prefix;
};

TEST_F(S3ServiceTest, ListBucketThrowExceptionWhenBucketStringIsEmpty) {
    EXPECT_THROW(s3service->ListBucket("", "", "", "", cred), std::runtime_error);
}

TEST_F(S3ServiceTest, ListBucketWithWrongRegion) {
    region = "noexist";
    EXPECT_EQ((void *)NULL, s3service->ListBucket(schema, region, "", "", cred));
}

TEST_F(S3ServiceTest, ListBucketWithWrongBucketName) {
    region = "us-west-2";
    bucket = "foo/bar";
    EXPECT_EQ((void *)NULL, s3service->ListBucket(schema, region, "", "", cred));
}

TEST_F(S3ServiceTest, ListBucketWithNormalBucket) {
    region = "us-west-2";
    bucket = "s3test.pivotal.io";
    EXPECT_NE((void *)NULL, s3service->ListBucket(schema, region, bucket, "", cred));
}