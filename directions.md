
# Welcome to contributors

 
This page collects early thoughts about how we plan to build this adapter.

# General idea

Glue will expose an Interactive Session API that works like this:
1. you create a Session, 
2. you send code to it
3. poll the results
4. and retrieve the results (txt based , no wire protocol for now)

We will mimic a cursor api on top of this interface and dbt will see
it as a DB Apuiconnection and run SQL queries against it.


As a dbt developer, you would then just provide the following in your project :
```yaml

target:
  type: glue
  query-comment: null
  role_arn: arn:aws:iam::<ACCOUNTID>:role/<GLUEROLENAME>
  region: <AWSREGION>
  workers: 5
  worker_type: G.1X
  schema: "dbt_test_{{ var('_dbt_random_suffix') }}"
  database: dbt
  session_id: s_xxxxx-xxx-xxx-xxx-xxx #optional
  session_provisionning_timeout_in_seconds: 40
  location: "s3://<BUCKET>/dbt_test_{{ var('_dbt_random_suffix') }}/"``` 
  
```

# How the code is structured 
We created the package using the dbt adapter plugin as explained in [dbt docs](https://docs.getdbt.com/docs/contributing/building-a-new-adapter#scaffolding-a-new-adapter).

 ```bash 
 $ python create_adapter_plugins.py --sql --title-case=MyAdapter ./ myadapter 
 ```
https://docs.getdbt.com/docs/contributing/building-a-new-adapter

The tree looks like this : 
```
|
+--- dbt/
    |
    +--- adapters/
    |
    +--- glue/ # contains the actual python code that handles connection
    |
    +--- include/ # handles SQL macros for the glue adapter
```


## adapters/glue

We have implemented most of the code here,
We have a basic DB API compatible implementation for Glue session in dbt/glue/adapters/gluedbapi.
The connection manager is pretty straight forward to use :
```python
 handle: GlueConnection= GlueConnection(credentials=credentials)
 handle.connect()
```



## include/
TBD



# Testing 

We were able to run some tests from the dbt provided tests  provided here :https://github.com/fishtown-analytics/dbt-adapter-tests
empty and base 

