# timon_pyo3

Efficient local storage and Amazon S3-compatible data synchronization for time-series data, leveraging Parquet for storage and DataFusion for querying, all wrapped in a simple and intuitive API.

## Description

`timon_pyo3` is a Python package that provides a high-performance time-series database interface built with Rust and exposed to Python via PyO3. It offers:

- Local storage with Parquet format
- Amazon S3-compatible cloud synchronization
- SQL querying via DataFusion
- Zero-copy data transfer with PyArrow integration
- Support for complex data types (arrays, nested structures)

## Prerequisites

Before building and running this project, ensure you have the following installed:

- **Python** >= 3.8
- **Rust** (latest stable version recommended)
- **Cargo** (Rust's package manager)
- **maturin** >= 1.7, < 2.0 (for building Python extensions from Rust)

### Installing Prerequisites

#### Install Rust and Cargo

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### Install maturin

```bash
pip install maturin
# or
cargo install maturin
```

#### Install Python Dependencies

The following Python packages are required for testing:

```bash
pip install pyarrow pandas
```

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd timon_pyo3
```

### 2. Create and Activate Virtual Environment

Create a Python virtual environment to isolate dependencies:

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On Linux/macOS:
source .venv/bin/activate

# On Windows:
# .venv\Scripts\activate
```

After activation, your terminal prompt should show `(.venv)` indicating the virtual environment is active.

### 3. Install Python Dependencies

```bash
pip install pyarrow pandas
```

## Building the Binary

### Option 1: Development Build (Recommended for Testing)

Build and install the package in development mode:

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Build and install in development mode
maturin develop
```

This will:
- Compile the Rust code
- Build the Python extension module
- Install it in your virtual environment

### Option 2: Build Release Wheel

Build an optimized release wheel:

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Build release wheel
maturin build --release
```

The wheel will be created in `target/wheels/` directory.

### Option 3: Install from Pre-built Wheel

If you have a pre-built wheel available:

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Install from wheel
pip install target/wheels/timon_pyo3-<version>-cp<python_version>-cp<python_version>-*.whl
```

Replace `<version>` and `<python_version>` with the appropriate values. For example:

```bash
pip install target/wheels/timon_pyo3-1.1.0-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

## Testing

### Run the Test Script

After building and installing the package, you can test it using the provided test script:

```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Run the test script
python src/lib.py
```

### Expected Output

The script will:
1. Initialize Timon with storage and S3 configuration
2. Create a database (`test_db`)
3. Create a table (`test_table`) with a defined schema
4. Insert sample time-series data
5. Query the data using SQL
6. Convert results to PyArrow Table and Pandas DataFrame
7. Display the results

You should see output similar to:

```
Timon Initialized Successfully
{"json_value":null,"message":"Database created successfully","status":200}
{"json_value":null,"message":"Table created successfully","status":200}
{"json_value":[],"message":"Records inserted successfully","status":200}
query_py_response > {"json_value":[...],"message":"query data with success...","status":200}
...
         date  step  calories  distance                            arraySteps
0  1739181600  1000      1.05      0.01      [18, 0, 0, 20, 0, 0, 0, 0, 0, 0]
...
```

### Troubleshooting

**Issue: Module not found**
- Ensure the virtual environment is activated
- Verify the package was built and installed: `pip list | grep timon-pyo3`

**Issue: Build errors related to Rust/Cargo**
- Ensure Rust and Cargo are properly installed: `rustc --version` and `cargo --version`
- Try cleaning the build: `cargo clean` and rebuild

**Issue: Dependency conflicts**
- Ensure you're using compatible versions of dependencies
- Check `Cargo.toml` for dependency versions

## Project Structure

```
timon_pyo3/
├── src/
│   ├── lib.rs          # Rust implementation (PyO3 bindings)
│   └── lib.py          # Python test script
├── Cargo.toml          # Rust dependencies
├── pyproject.toml      # Python package configuration
├── README.md           # This file
└── target/             # Build artifacts (generated)
    └── wheels/         # Built Python wheels
```

## Usage Example

```python
import timon_pyo3
import pyarrow as pa

# Initialize Timon
response = timon_pyo3.init(
    storage_path="tmp/timon",
    bucket_interval=30,
    username="your_username",
    bucket_endpoint="https://your-s3-endpoint.com",
    bucket_name="your-bucket",
    access_key_id="your-access-key",
    secret_access_key="your-secret-key",
    bucket_region="us-west-2",
)

# Create database
timon_pyo3.create_database_py("my_db")

# Create table with schema
schema = '{"timestamp": {"type": "int", "required": true, "datetime": true}, ...}'
timon_pyo3.create_table_py("my_db", "my_table", schema)

# Insert data
json_data = '[{"timestamp": "2025.02.10 10:00:00", ...}]'
timon_pyo3.insert_py("my_db", "my_table", json_data)

# Query data
result = timon_pyo3.query_py("my_db", "SELECT * FROM my_table")

# Query as DataFrame (PyArrow)
df_reader = timon_pyo3.query_df_py("my_db", "SELECT * FROM my_table")
table = pa.table(df_reader)
df = table.to_pandas()
```

## Deployment to PyPI

### Prerequisites

- PyPI account ([register](https://pypi.org/account/register/))
- `pip install twine`
- Update version in `Cargo.toml` and `pyproject.toml`

### Build Wheels

**Local build:**
```bash
source .venv/bin/activate
maturin build --release
```

**Multiple Python versions (Docker):**
```bash
for version in 3.8 3.9 3.10 3.11 3.12; do
    docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release --interpreter python${version}
done
```

Wheels are created in `target/wheels/`.

### Get PyPI API Token

1. Go to [pypi.org/manage/account/](https://pypi.org/manage/account/) → API tokens
2. Create token (starts with `pypi-`)
3. Set credentials:
```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=$(cat ~/.ssh/PyPi_token)
```

### Upload to PyPI:

```bash
twine upload target/wheels/timon_pyo3-<version>-*.whl
```

Replace `<version>` with your version (e.g., `1.1.0`).

### Verify

```bash
pip install timon-pyo3
python -c "import timon_pyo3; print(timon_pyo3.__version__)"
```

### Troubleshooting

- **"File already exists"**: Update version number and rebuild
- **"Invalid credentials"**: Use `__token__` as username, verify token
- **"Wheel not found"**: Check `ls target/wheels/` and version number


## License

MIT License - see LICENSE file for details

## Author

Ahmed Boutaraa (ahmed@mongrov.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
