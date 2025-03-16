import timon_pyo3
import pyarrow as pa

response = timon_pyo3.init(
    "tmp/timon",
    30,
    "ahmed_test",
    "https://monrudolfs.s3.amazonaws.com",
    "rapids",
    "xxxx",
    "xxxx",
    "us-west-2",
)
print(response)

db_response = timon_pyo3.create_database_py("test_db")
print(db_response)

table_schema = """
    {
      "date": {
        "type": "int",
        "required": true,
        "unique": true,
        "datetime": true
      },
      "distance": {
        "type": "int|float"
      },
      "step": {
        "type": "int"
      },
      "calories": {
        "type": "int|float"
      },
      "arraySteps": {
        "type": "array"
      },
      "is_sync": {
        "type": "bool"
      }
    }
"""
table_response = timon_pyo3.create_table_py("test_db", "test_table", table_schema)
print(table_response)

json_data = """
    [
      {"date":"2025.02.10 10:00:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":1.05,"distance":0.01,"step":1000},
      {"date":"2025.02.10 10:01:00","arraySteps":[43,39,0,0,0,0,0,0,0,0],"calories":2.56,"distance":0.05,"step":1001},
      {"date":"2025.02.10 10:02:00","arraySteps":[20,0,0,0,0,0,0,0,0,0],"calories":0.61,"distance":0.01,"step":1002},
      {"date":"2025.02.10 10:03:00","arraySteps":[19,0,0,0,0,0,0,0,0,0],"calories":0.65,"distance":0.01,"step":1003},
      {"date":"2025.02.10 10:21:00","arraySteps":[54,33,2,0,0,0,0,0,0,0],"calories":2.83,"distance":0.06,"step":1021},
      {"date":"2025.02.10 10:25:00","arraySteps":[38,0,0,15,0,0,0,0,0,0],"calories":1.53,"distance":0.03,"step":1025},
      {"date":"2025.02.10 10:30:00","arraySteps":[50,16,0,55,23,0,0,18,46,0],"calories":6.19,"distance":0.14,"step":1030},
      {"date":"2025.02.10 10:31:00","arraySteps":[18,0,0,20,0,0,0,0,0,0],"calories":1.05,"distance":0.01,"step":1031}
    ]
"""
insert_response = timon_pyo3.insert_py("test_db", "test_table", json_data)
print(insert_response)

query_py_response = timon_pyo3.query_py("test_db", "SELECT * FROM test_table")
print("query_py_response >", query_py_response)

query_df_py_response = timon_pyo3.query_df_py("test_db", "SELECT * FROM test_table")
print(query_df_py_response, ">>>")

pyarray = pa.table(query_df_py_response)
print(pyarray)

df = pyarray.to_pandas()
print(df)
