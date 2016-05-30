#include "gtest/gtest.h"
#include "s3bucket_reader.cpp"

TEST(S3BucketReader, OpenInvalidURL) {
    S3BucketReader *reader = new S3BucketReader();

    string url = "https://s3-us-east-2.amazon.com/s3test.pivotal.io/whatever";

    reader->setUrl(url);
    EXPECT_THROW(reader->open(), std::runtime_error);
}

TEST(S3BucketReader, OpenURL) {
	S3BucketReader *reader = new S3BucketReader();
	string url = "https://s3-us-east-2.amazonaws.com/s3test.pivotal.io/whatever";
	reader->setUrl(url);

	EXPECT_NO_THROW(reader->open());
}
