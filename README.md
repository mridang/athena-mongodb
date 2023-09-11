# MongoDB connector for Athena Federation

An enhanced version of the DocDB connector for AWS Athena. 

This project was started as the current DocDB connector for AWS Athena did 
not support multi-tenant collections.

A few of the places I've worked at used tenant-specific collections e.g `Product_1`, `Product_2`

This connector adds support for multi-tenant collections by providing a "view" of all
underlying multi-tenant collections.

Other improvements include:

* An improved test suite backed by a MongoDB test-container as the previous one heavily relied on mocks and stubs
* Support for AWS Lambda Snapstart as this is now supported on Lambda.
* Support for ZSTD and Gzip compression as this requires shared libraries such as libzstd and libgzip to be bundled.
* Enhanced logging and improved configuration as the previous implementation did not expose tunable sampling parameters

## Installation

Unfortunately, the connector is not available in any public Maven repositories except the GitHub Package Registry. 
For more information on how to install packages from the GitHub Package
Registry, [https://docs.github.com/en/packages/guides/configuring-gradle-for-use-with-github-packages#installing-a-package][see the GitHub docs]


## Usage


* `SCHEMA_INFERENCE_NUM_DOCS`: Defines the number of documents that should be 
sampled to infer the schema. Default `10`.
*  `MONGO_QUERY_BATCH_SIZE`: Defines the number of documents to fetch from MongoDB
in every batch. Default `100`.
* `GLOB_PATTERN`: Defines how collections should be coalesced together 
when multi-tenant support is required. The glob pattern is a valid regex with 
the leading and trailing regex anchor characters omitted i.e. `$` and `^`. 
If you have multi-tenant collections in the form



## Caveats

The current implementation doesn't


5. Implement paritioning
7. Add support for compression
8. Rename namespace

In the event that these are needed, upstream pull-requests are welcomed.

## Authors

* Mridang Agarwalla <mridang.agarwalla@gmail.com>
* Palantir Technologies
* Amazon Web Services

## License

Apache-2.0 License

[see the GitHub docs]: https://docs.github.com/en/packages/guides/configuring-gradle-for-use-with-github-packages#installing-a-package
