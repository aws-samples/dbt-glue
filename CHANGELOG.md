## dbt-glue 1.0.0 (Release TBD)

## v0.3.1 (unreleased)
- Include config `full_refresh` flag when materialization is incremental

## v0.3.0
- Updated dependencies to support dbt-core 1.3.0

## v0.2.15
- Force database parameter must be omitted or have the same value as schema  [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/93)

## v0.2.14

- Fix duplicates when using partitions changes with Hudi/Merge incremental materialization  [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/90)

## v0.2.12

- Added a function to add an end space in case of single quote at the end of a query. Ex: WHERE column='foo' [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/87)

## v0.2.11

- [#80](https://github.com/aws-samples/dbt-glue/pull/80): Fix default glue version on documentation
  - Changing default glue version and fixing a typo. [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/80)
  - Changing on Readme file the pip and python commands by python3 and pip3. This resolves potential issues when python2 is installed too.

## v0.2.1

- [#45](https://github.com/aws-samples/dbt-glue/pull/45): Modified Connection argument for Glue Session and table relation information for incremental mode
  - Modified Connection argument for Glue Session
  - Updated get_relation method to return relation as dict instead of list. [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/52)
  - Added Conf param for Glue to add custom spark configuration options.
  - Updated glue.sql.sources.partitionOverwriteMode to spark.sql.sources.partitionOverwriteMode to work partition overwrite properly.
- Override default types for STRING from TEXT to STRING