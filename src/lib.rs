use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use tsdb_timon::timon_engine::{
    cloud_sync_parquet, create_database, create_table, delete_database, delete_table, init_bucket,
    init_timon, insert, list_databases, list_tables, query, query_bucket,
};

lazy_static::lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
}

#[pyfunction]
fn init(
    storage_path: String,
    bucket_endpoint: String,
    bucket_name: String,
    access_key_id: String,
    secret_access_key: String,
    bucket_region: String,
) -> PyResult<String> {
    TOKIO_RUNTIME.block_on(async {
        // Initialize Timon storage
        init_timon(&storage_path, 15).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to initialize Timon: {}", e))
        })?;

        init_bucket(
            &bucket_endpoint,
            &bucket_name,
            &access_key_id,
            &secret_access_key,
            &bucket_region,
        )
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to initialize Timon Bucket: {}",
                e
            ))
        })?;
        Ok("Timon Initialized Successfully".to_string())
    })
}

#[pyfunction]
fn create_database_py(db_name: String) -> PyResult<String> {
    // Create a database
    let result = create_database(&db_name).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create database: {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn create_table_py(db_name: String, table_name: String, schema: String) -> PyResult<String> {
    // Define table schema and create a table
    let result = create_table(&db_name, &table_name, &schema).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create table: {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn list_databases_py() -> PyResult<String> {
    let result = list_databases().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list databases: {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn list_tables_py(db_name: String) -> PyResult<String> {
    let result = list_tables(&db_name).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list tables: {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn delete_database_py(db_name: String) -> PyResult<String> {
    let result = delete_database(&db_name).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to delete database {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn delete_table_py(db_name: String, table_name: String) -> PyResult<String> {
    let result = delete_table(&db_name, &table_name).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to delete table {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn insert_py(db_name: String, table_name: String, json_data: String) -> PyResult<String> {
    let result = insert(&db_name, &table_name, &json_data).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create table: {}", e))
    })?;
    Ok(result.to_string())
}

#[pyfunction]
fn cloud_sync_parquet_py(
    username: String,
    db_name: String,
    table_name: String,
) -> PyResult<String> {
    TOKIO_RUNTIME.block_on(async {
        let result = cloud_sync_parquet(&username, &db_name, &table_name)
            .await
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to Sink Daily Data: {}",
                    e
                ))
            })?;
        Ok(result.to_string())
    })
}

#[pyfunction]
fn query_py(db_name: String, sql_query: String) -> PyResult<String> {
    TOKIO_RUNTIME
        .block_on(async {
            query(&db_name, &sql_query).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Query failed: {}", e))
            })
        })
        .map(|value| value.to_string())
}

#[pyfunction]
fn query_bucket_py(
    username: String,
    sql_query: String,
    date_range: HashMap<String, String>,
) -> PyResult<String> {
    TOKIO_RUNTIME
        .block_on(async {
            let converted_date_range: HashMap<&str, &str> = date_range
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            query_bucket(&username, &sql_query, converted_date_range)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("Query failed: {}", e))
                })
        })
        .map(|value| value.to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn timon_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init, m)?)?;
    m.add_function(wrap_pyfunction!(create_database_py, m)?)?;
    m.add_function(wrap_pyfunction!(create_table_py, m)?)?;
    m.add_function(wrap_pyfunction!(list_databases_py, m)?)?;
    m.add_function(wrap_pyfunction!(list_tables_py, m)?)?;
    m.add_function(wrap_pyfunction!(delete_database_py, m)?)?;
    m.add_function(wrap_pyfunction!(delete_table_py, m)?)?;
    m.add_function(wrap_pyfunction!(insert_py, m)?)?;
    m.add_function(wrap_pyfunction!(cloud_sync_parquet_py, m)?)?;
    m.add_function(wrap_pyfunction!(query_py, m)?)?;
    m.add_function(wrap_pyfunction!(query_bucket_py, m)?)?;
    Ok(())
}
