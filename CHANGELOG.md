## dbt-glue 1.0.0 (Release TBD)

## v0.2.11 (unreleased)

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