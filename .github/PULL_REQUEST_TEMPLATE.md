resolves #247

<!---
  Include the number of the issue addressed by this PR above if applicable.
  PRs for code changes without an associated issue *will not be merged*.
  See CONTRIBUTING.md for more information.

  Example:
    resolves #1234
-->

### Description

Fixing [full refresh with iceberg issue](https://github.com/aws-samples/dbt-glue/issues/247).

In the `incremental.sql` file, it was first checking if the relation was iceberg, instead of checking for full refresh.
See [athena connector implementation](https://github.com/dbt-athena/dbt-athena/blob/main/dbt/include/athena/macros/materializations/models/incremental/incremental.sql) for reference.

We fix it by modifying the order of the condition, we first check if the user has used the full refresh flag (i.e. `-f` or `--full-refresh`).

To check if the user used the flag, we use the dbt core macro [should_full_refresh()](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/include/global_project/macros/materializations/configs.sql#L6) instead of the currently implemented *full_refresh_mode* variable. Since we don't need the *full_refresh_mode* variable anymore, we remove this variable from the code.

Once the full refresh condition is True, we check if it is an iceberg materialization. If yes, we first delete the table using `glue__drop_relation` macro in adapters.sql.
For this macro to work, we slightly modify this macro with a special bracket character to avoid an `analysisexception: spark_catalog requires a single-part namespace` error.
Finally, we write the table with the `iceberg_write` function, that will re-create the table from scratch since it does not exist anymore.

TLDR:
- We modify the order of condition by first checking if full refresh is enabled instead of if iceberg.
- We check if full refresh is enabled with a dbt-core macro instead of custom implementation.
- If full refresh and iceberg, then we delete the table.
- We re-write the table using iceberg write.

### Checklist

- [X] I have signed the [CLA](https://docs.getdbt.com/docs/contributor-license-agreements)
- [X] I have run this code in development and it appears to resolve the stated issue
- [ ] This PR includes tests, or tests are not required/relevant for this PR
- [ ] I have updated the `CHANGELOG.md` and added information about my change to the "dbt-glue next" section.


By submitting this pull request, I confirm that you can use, modify, copy, and redistribute this contribution, under the terms of your choice.
