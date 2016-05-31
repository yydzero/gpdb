#include "s3bucket_reader.cpp"
#include "s3interface.cpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

class MockS3Interface : public S3Interface {
   public:
    MOCK_METHOD5(ListBucket,
                 ListBucketResult*(const string& schema, const string& region,
                                   const string& bucket, const string& prefix,
                                   const S3Credential& cred));
};

class S3BucketReaderTest : public testing::Test {
  protected:
	// Remember that SetUp() is run immediately before a test starts.
	virtual void SetUp() {
		reader = new S3BucketReader();
		reader->setS3interface(&s3interface);
	}

	// TearDown() is invoked immediately after a test finishes.
	virtual void TearDown() {
	}

	S3BucketReader* reader;
	MockS3Interface s3interface;
};

TEST_F(S3BucketReaderTest, OpenInvalidURL) {
    string url = "https://s3-us-east-2.amazon.com/s3test.pivotal.io/whatever";

    reader->setUrl(url);
    EXPECT_THROW(reader->open(), std::runtime_error);
}

TEST_F(S3BucketReaderTest, OpenURL) {
    ListBucketResult result;

    EXPECT_CALL(s3interface, ListBucket(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(&result));

    string url = "https://s3-us-east-2.amazonaws.com/s3test.pivotal.io/whatever";
    reader->setUrl(url);

    EXPECT_NO_THROW(reader->open());
}

TEST_F(S3BucketReaderTest, ListBucketWithRetryThrowException) {
    EXPECT_THROW(reader->listBucketWithRetry(0), std::runtime_error);
}

TEST_F(S3BucketReaderTest, ListBucketWithRetryThrowExceptionWhenS3InterfaceIsNULL) {
	reader->setS3interface(NULL);
    EXPECT_THROW(reader->listBucketWithRetry(1), std::runtime_error);
}

TEST_F(S3BucketReaderTest, ListBucketWithRetry) {
    ListBucketResult result;

    EXPECT_CALL(s3interface, ListBucket(_, _, _, _, _))
        .Times(1)
        .WillOnce(Return(&result));

    EXPECT_NE((void*)NULL, reader->listBucketWithRetry(1));
}

TEST_F(S3BucketReaderTest, ListBucketWithRetries) {
    ListBucketResult result;

    EXPECT_CALL(s3interface, ListBucket(_, _, _, _, _))
        .Times(3)
		.WillOnce(Return((ListBucketResult *)NULL))
		.WillOnce(Return((ListBucketResult *)NULL))
        .WillOnce(Return(&result));

    EXPECT_EQ(&result, reader->listBucketWithRetry(3));
}