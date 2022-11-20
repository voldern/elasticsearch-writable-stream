# Change Log

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [2.0.2] - 20-11-2022

- Making callback changes to be compatible to @elastic/elasticsearch 8.5.0 library, however need to make exception handling changes to for bulkWrite

## [2.0.1] - 2017-08-22

### Fixed

- Support error objects returned by newer versions of ES.
  https://github.com/voldern/elasticsearch-writable-stream/issues/4

## [2.0.0] - 2016-09-13

### Changed

- Renamed library to `elasticsearch-writable-stream`

### Added

- Support for `update_by_query`

## [1.0.0] - 2016-08-18

### Added

- Make it possible to specify the bulk action

## [0.3.0] - 2016-07-22

### Added

- Make it possible to add parent property to documents

## [0.2.1] - 2016-05-26

### Fixed

- Clear flush timer when stream has ended

## [0.2.0] - 2016-03-09

### Added

- `flushTimeout` option what will flush records in the queue after
  given time interval if highWaterMark hasn't been reached

## [0.1.2] - 2015-11-16

### Fixed

- Log right amount of records. The records count that was logged was
  doubled in previous release.

## [0.1.1] - 2015-11-16

### Fixed

- Do not write if there are no records in the queue when the stream gets closed

## [0.1.0] - 2015-11-16

### Changed

- Add property `records` to error events which contains the records
  that couldn't be written

## [0.0.4] - 2015-10-28

### Added

- Keywords to package.json

## [0.0.3] - 2015-10-28

### Changed

- Updated README

### Added

- API documentation

## [0.0.2] - 2015-10-27

### Added

- Repository field to package.json

## [0.0.1] - 2015-10-27

Initial release
