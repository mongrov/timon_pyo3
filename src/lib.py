# Checkout: https://github.com/PyO3/pyo3
import timon_pyo3  # type: ignore
import json

timon_pyo3.init(
    "tmp/timon",
    "http://localhost:9000",
    "timon",
    "ahmed",
    "ahmed1234",
    "us-east-1",
)
print("*** Timon Initialized ***")

create_db_result = timon_pyo3.create_database_py("test_db")
print("create_db_result =>", create_db_result)

create_table_result = timon_pyo3.create_table_py(
    "test_db",
    "test_table",
    """{
    "date": {"type": "string", "required": true, "unique": true},
    "temperature": {"type": "float", "required": true},
    "relativeHumidity": {"type": "float", "required": true}
}""",
)
print("create_table_result =>", create_table_result)

insert_result = timon_pyo3.insert_py(
    "test_db",
    "test_table",
    json.dumps(
        [
            {
                "date": "2025-01-10T19:37:04.663Z",
                "temperature": 23.5,
                "relativeHumidity": 12.5,
            }
        ]
    ),
)
print("insert_result =>", insert_result)

query_result = timon_pyo3.query_py(
    "test_db", "SELECT * FROM test_table ORDER BY date ASC LIMIT 10"
)
print("query_result =>", query_result)

query_bucket_result = timon_pyo3.query_bucket_py(
    "test_db",
    "SELECT * FROM test_table ORDER BY date ASC LIMIT 10",
    {"start_date": "2025-01-10", "end_date": "2025-01-20"},
)
print("query_bucket_result =>", query_bucket_result)

cloud_sync_parquet_result = timon_pyo3.cloud_sync_parquet_py(
    "userpyo3", "test_db", "test_table"
)
print("cloud_sync_parquet_result =>", cloud_sync_parquet_result)
