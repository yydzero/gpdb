#include "s3interface.cpp"
#include "restful_service.cpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::Throw;
using ::testing::_;

class MockS3RESTfulService : public RESTfulService {
public:
    MOCK_METHOD3(get, Response(const string& url, const HTTPHeaders& headers,
                     const map<string, string>& params));
};

class XMLGenerator {
public:
    XMLGenerator() : isTruncated(false) {
    }

    XMLGenerator* setName(string name) {
        this->name = name;
        return this;
    }
    XMLGenerator* setPrefix(string prefix) {
        this->prefix = prefix;
        return this;
    }
    XMLGenerator* setMarker(string marker) {
        this->marker = marker;
        return this;
    }
    XMLGenerator* setMaxKeys(string maxKeys) {
        this->maxKeys = maxKeys;
        return this;
    }
    XMLGenerator* setIsTruncated(bool isTruncated) {
        this->isTruncated = isTruncated;
        return this;
    }
    XMLGenerator* pushBuckentContent(BucketContent content) {
        this->contents.push_back(content);
        return this;
    }

    vector<uint8_t> toXML() {
        stringstream sstr;
        sstr << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
             << "<ListBucketResult>"
             << "<Name>" << name << "</Name>"
             << "<Prefix>" << prefix << "</Prefix>"
             << "<Marker>" << marker << "</Marker>"
             << "<MaxKeys>" << maxKeys << "</MaxKeys>"
             << "<IsTruncated>" << (isTruncated ? "true" : "false") << "</IsTruncated>";

        for (vector<BucketContent>::iterator it = contents.begin(); it != contents.end(); it++) {
            sstr << "<Contents>"
                        << "<Key>" << it->name << "</Key>"
                        << "<Size>" << it->size << "</Size>"
                 << "</Contents>";
        }
        sstr << "</ListBucketResult>";
        string xml = sstr.str();
        return vector<uint8_t>(xml.begin(), xml.end());
    }

private:
    string name;
    string prefix;
    string marker;
    string maxKeys;
    bool isTruncated;

    vector<BucketContent> contents;
};

class S3ServiceTest : public testing::Test {
   protected:
    // Remember that SetUp() is run immediately before a test starts.
    virtual void SetUp() {
        s3ext_logtype = STDERR_LOG;
        s3ext_loglevel = EXT_INFO;

        s3service = new S3Service();
        schema = "https";
        s3service->setRESTfulService(&mockRestfulService);
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

    MockS3RESTfulService mockRestfulService;
    Response response;
};

TEST_F(S3ServiceTest, ListBucketThrowExceptionWhenBucketStringIsEmpty) {
    EXPECT_THROW(s3service->ListBucket("", "", "", "", cred), std::runtime_error);
}

TEST_F(S3ServiceTest, ListBucketWithWrongRegion) {
    EXPECT_CALL(mockRestfulService, get(_, _, _))
            .WillOnce(Return(response));

    EXPECT_THROW(s3service->ListBucket(schema, "nonexist", "", "", cred), std::runtime_error);
}

TEST_F(S3ServiceTest, ListBucketWithWrongBucketName) {
    region = "us-west-2";
    bucket = "foo/bar";
    uint8_t xml[] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Error>"
            "<Code>PermanentRedirect</Code>"
            "<Message>The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.</Message>"
            "<Bucket>foo</Bucket><Endpoint>s3.amazonaws.com</Endpoint>"
            "<RequestId>27DD9B7004AF83E3</RequestId>"
            "<HostId>NL3pyGvn+FajhQLKz/hXUzV1VnFbbwNjUQsqWeFiDANkV4EVkh8Kpq5NNAi27P7XDhoA9M9Xhg0=</HostId>"
            "</Error>";
    vector<uint8_t> raw(xml, xml + sizeof(xml) - 1);
    response.getRawData() = raw;
    EXPECT_CALL(mockRestfulService, get(_, _, _))
            .WillOnce(Return(response));

    EXPECT_EQ((void *)NULL, s3service->ListBucket(schema, region, "", "", cred));
}

TEST_F(S3ServiceTest, ListBucketWithNormalBucket) {
    region = "us-west-2";
    bucket = "s3test.pivotal.io";

    XMLGenerator *gen = new XMLGenerator();
    gen->setName("s3test.pivotal.io")->setPrefix("threebytes/")->setMaxKeys("1000")->setIsTruncated(false)
       ->pushBuckentContent(BucketContent("threebytes/", 0))
       ->pushBuckentContent(BucketContent("threebytes/threebytes", 3));

    response.getRawData() = gen->toXML();

    EXPECT_CALL(mockRestfulService, get(_, _, _))
        .WillOnce(Return(response));

    ListBucketResult *result = s3service->ListBucket(schema, region, bucket, "threebytes/", cred);
    EXPECT_NE((void *)NULL, result);
    EXPECT_EQ(1, result->contents.size());
}