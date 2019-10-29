## 0.0.18 (2019-10-29)

Features:

- Added support for Elasticsearch 7.x. NOTE: Elasticsearch 5.x has reached its
  end of life and the support for it will be removed in a future release.

## 0.0.17 (2019-09-11)

Bug fixes:

- Fixed error reporting for bulk indexing operations with newer versions of
  Elasticsearch to include the error details.

## 0.0.16 (2019-04-16)

Improvements:

- Update for the 0.1.5 version of the ContainerCrawler library.

## 0.0.15.1 (2018-12-21)

Improvements:

- Allow for using an older release of elasticsearch-py.

## 0.0.15 (2018-12-17)

Features:

- Allow for specifying whether Elasticsearch server's SSL certificate should be
  verified by specifying the `verify_certs` mapping option.
- Added a mapping option -- `ca_certs` -- to specify the certificate authority
  (CA) bundle to use for verifying the Elasticsearch server's certificate.

Bug fixes:

- Fixed an issue where static large objects could not be indexed. The fix
  applies to other boolean fields as well.

Improvements:

- Updated to version 0.1.1 of the ContainerCrawler library.

## 0.0.14 (2018-11-29)

Improvements:

- Update for the 0.1.0 version of the ContainerCrawler library.

## 0.0.13 (2018-10-15)

Improvements:

- Update for the 0.0.17 version of the ContainerCrawler library.

# 0.0.12 (2018-10-07)

Improvements:

- Update for the new ContainerCrawler library release.

## 0.0.11 (2018-06-08)

Features:

- Allow for parsing JSON from metadata values.
- Operators can define the Elasticsearch pipeline to be used when indexing
  documents.

## 0.0.10 (2017-09-11)

Bug fixes:

- Fix an issue where objects with unicode names cannot be indexed if
  simple-json is not installed (as the UTF-8 encoded strings may be
  attempted to be encoded one more time).
- Handles a missing type error gracefully, which may occur if the index was
  created without a mapping. In that case, the "object" document type is
  created with the default mappings.

## 0.0.9 (2017-07-13)

Bug fixes:

- change the constructor to work with the changes to the Container Crawler.

## 0.0.8 (2017-07-12)

Bug fixes:

- properly handle unicode characters in object names.

## 0.0.7 (2017-06-12)

Improvement:

- do not rely on the deprecated "found" field in DELETE responses.
- use "text" or "keyword" fields when creating mappings for Swift objects
  (as opposed to "string"). We will now check the Elasticsearch version and
  only use "string" with Elasticsearch servers < 5.x.
- bump the Elasticsearch library requirement to 5.x.

Bug fixes:

- change the document ID to be SHA256 of account, container, and object. The
  string is concatenated with the "/" separator. This will ensures that we
  can work with long object names and Elasticsearch version 5.0 or newer,
  which reject IDs longer than 512 characters.

## 0.0.6 (2017-05-05)

Improvements:

- change the handle() to conform to the new API in ContainerCrawler.

## 0.0.5 (2017-04-24)

Bug fixes:

- exit with an error code of 0 and a message if the configuration file does
  not exist. This may happen on a fresh installation of the daemon.

## 0.0.4 (2017-02-28)

Bug fixes:

- use long for the object size; otherwise, we are limited to reporting
  objects only up to 2GB.

## 0.0.3 (2017-02-06)

Bug fixes:

- fix the conversion of last-modified to a proper Elasticsearch date format
  (milliseconds from epoch, as opposed to the mistaken use of seconds).

Improvements:

- better error messages on failure to index (includes the "reason" from
  Elasticsearch).
