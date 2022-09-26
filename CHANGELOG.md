## dbt-glue 1.0.0 (Release TBD)

## v0.2.1 (unreleased)

- [#45](https://github.com/aws-samples/dbt-glue/pull/45): Modified Connection argument for Glue Session and table relation information for incremental mode
  - Modified Connection argument for Glue Session
  - Updated get_relation method to return relation as dict instead of list. [Github Issue Link](https://github.com/aws-samples/dbt-glue/issues/52)
  - Added Conf param for Glue to add custom spark configuration options.
  - Updated glue.sql.sources.partitionOverwriteMode to spark.sql.sources.partitionOverwriteMode to work partition overwrite properly.
- Override default types for STRING from TEXT to STRING