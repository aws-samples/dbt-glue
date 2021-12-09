from dbt.adapters.glue.gluedbapi import GlueConnection, GlueCursor, GlueDictCursor
import boto3
import uuid

account = "xxxxxxxxxxxx"
s3bucket = "s3://dbtbucket/dbttessample/"

def __test_connect(session: GlueConnection):
    print(session.session_id)
    assert session.session_id is not None


def __test_cursor(session: GlueConnection):
    cursor: GlueCursor = session.cursor()
    cursor.execute("select 1 as A")
    assert cursor.columns == ["A"]
    for row in cursor:
        assert row[0] == 1


def __test_list_schemas(session: GlueConnection):
    cursor: GlueCursor = session.cursor()
    cursor.execute("show databases")
    found_default = False
    for record in cursor:
        print(record)
        if record[0] == "default":
            found_default = True
    assert found_default


def __test_list_relations(session: GlueConnection):
    cursor: GlueCursor = session.cursor()
    cursor.execute("show tables")
    for r in cursor:
        print(r)

    assert True


def __test_get_columns_in_relation(session: GlueConnection):
    cursor: GlueCursor = session.cursor()
    cursor.execute("describe my_first_dbt_model")
    for r in cursor:
        print(r)

    assert True


def __test_query_with_comments(session):
    cursor: GlueCursor = session.cursor()
    cursor.execute('''show databases''')
    for r in cursor:
        print(r)

    assert True


def test_create_database(session, region):
    client = boto3.client("glue", region_name=region)
    schema = "testdb111222333"
    try:
        response = client.create_database(
            DatabaseInput={
                "Name": f"{schema}",
                'Description': 'test dbt database',
                'LocationUri': s3bucket
            }
        )

        lf = boto3.client("lakeformation", region_name=region)
        Entries = []
        for i, role_arn in enumerate([
            session.credentials.role_arn,
            f"arn:aws:iam::{account}:role/GlueInteractiveSessionRole",
            f"arn:aws:iam::{account}:user/cdkuser"]):
            Entries.append(
                {
                    "Id": str(uuid.uuid4()),
                    "Principal": {"DataLakePrincipalIdentifier": role_arn},
                    "Resource": {
                        "Database": {
                            # 'CatalogId': AWS_ACCOUNT,
                            "Name": schema,
                        }
                    },
                    "Permissions": [
                        "Alter".upper(),
                        "Create_table".upper(),
                        "Drop".upper(),
                    ],
                    "PermissionsWithGrantOption": [
                        "Alter".upper(),
                        "Create_table".upper(),
                        "Drop".upper(),
                    ],
                }
            )
            Entries.append(
                {
                    "Id": str(uuid.uuid4()),
                    "Principal": {"DataLakePrincipalIdentifier": role_arn},
                    "Resource": {
                        "Table": {
                            "DatabaseName": schema,
                            "TableWildcard": {},
                            "CatalogId": account
                        }
                    },
                    "Permissions": ["SELECT"],
                    "PermissionsWithGrantOption": ["SELECT"],
                }
            )
        lf.batch_grant_permissions(CatalogId=account, Entries=Entries)

    except client.exceptions.AlreadyExistsException:
        print("already exists")

    cursor: GlueCursor = session.cursor()
    response = cursor.execute(f"""
    create table {schema}.test(a string) 
     USING CSV
     LOCATION '{s3bucket}/table1/'
    """)
    print(response)
