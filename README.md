# Rust Query Engine Greatest Function

## Overview

The **Rust Query Engine Greatest Function** project implements the `GREATEST` SQL function within the [DataFusion](https://github.com/apache/datafusion) query engine using Rust. This implementation aims to mirror the behavior of Spark's `GREATEST` function, ensuring consistent results across both platforms. Additionally, the project provides Python bindings for seamless integration and a command-line interface (CLI) for executing SQL scripts.

## Features

- **Rust Implementation:** Efficient and type-safe implementation of the `GREATEST` function for various data types, including numeric, string, boolean, date, and timestamp types.
- **Python Bindings:** Expose the Rust `GREATEST` function to Python using [PyO3](https://pyo3.rs/), allowing Python applications to leverage the functionality.
- **Comprehensive Testing:** A robust test suite comparing the Rust implementation against Spark's `GREATEST` function to ensure accuracy and consistency.
- **DataFusion Integration:** Seamlessly integrate with DataFusion, enabling the use of the `GREATEST` function within SQL queries.

## Table of Contents

- [Documentation](documentation)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Building and Running the Rust Project](#building-and-running-the-rust-project)
  - [Setting Up Python Bindings](#setting-up-python-bindings)
  - [Running Python Tests](#running-python-tests)
  - [Building and Testing DataFusion Functions](#building-and-testing-datafusion-functions)
  - [Building and Using the DataFusion CLI](#building-and-using-the-datafusion-cli)
- [Usage](#usage)
  - [Running the Rust Main](#running-the-rust-main)
  - [Using the Python Module](#using-the-python-module)
  - [Executing SQL Scripts with the CLI](#executing-sql-scripts-with-the-cli)
- [Contributing](#contributing)
- [License](#license)

## Documentation

### Description

The `GREATEST` function returns the greatest value among a list of column names for each row, skipping `NULL` values. This function takes at least two parameters and will return `NULL` if all parameters are `NULL`.

**New in version 1.5.0.**

**Changed in version 3.4.0:** Supports Spark Connect.

### Parameters

- `*cols: ColumnOrName`: Two or more columns or expressions to compare. All columns should be of compatible data types.

### Returns

- **Column:** The greatest value among the provided columns for each row. The return type is determined based on type coercion rules similar to Spark's `GREATEST` function.


## Prerequisites

Before getting started, ensure you have the following installed on your system:

- **Rust:** Install Rust from [rustup.rs](https://rustup.rs/).
- **Python:** Version 3.8 or higher. Download from [python.org](https://www.python.org/downloads/).
- **Maturin:** Install via pip:

  ```bash
  pip install maturin
  ```

- **Apache Spark:** Ensure Spark is installed and accessible if you plan to compare results with Spark's `GREATEST` function.

## Installation

### Building and Running the Rust Project

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/rust-query-engine-greatest.git
   cd rust-query-engine-greatest
   ```

2. **Build the Project:**

   ```bash
   cargo build
   ```

3. **Run the Rust Main:**

   Execute the main Rust program to see sample outputs of the `GREATEST` function across different data types.

   ```bash
   cargo run
   ```

   **Sample Output:**

   ```
   Greatest Int: PrimitiveArray<Int32>
   [
     7,
     5,
     6,
   ]
   Greatest Float: PrimitiveArray<Float64>
   [
     3.3,
     2.2,
     4.4,
   ]
   Greatest Boolean: BooleanArray
   [
     true,
     true,
     true,
   ]
   Greatest UTF8: StringArray
   [
     "g",
     "h",
     "i",
   ]
   Greatest Dates: PrimitiveArray<Date32>
   [
     2020-03-01,
     2020-06-01,
     2020-07-01,
   ]
   Greatest Timestamps: PrimitiveArray<Timestamp(Nanosecond, None)>
   [
     2020-03-01T15:30:00,
     2020-06-01T08:00:00,
     2020-07-01T10:15:00,
   ]
   ```

### Setting Up Python Bindings

1. **Create a Python Virtual Environment:**

   ```bash
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment:**

   ```bash
   source venv/bin/activate
   ```

3. **Build and Install Python Bindings with Maturin:**

   ```bash
   maturin develop
   ```

   **Output:**

   ```
   🔗 Found pyo3 bindings
   🐍 Found CPython 3.12 at /path/to/venv/bin/python
      Compiling pyo3-build-config v0.21.2
      Compiling pyo3-macros-backend v0.21.2
      ...
      Compiling rust-query-engine-greatest v0.1.0 (/path/to/rust-query-engine-greatest)
       Finished dev profile [unoptimized + debuginfo] target(s) in 6.43s
   📦 Built wheel for CPython 3.12 to /tmp/.tmpWrTfnt/rust_query_engine_greatest-0.1.0-cp312-cp312-linux_x86_64.whl
   ✏️  Setting installed package as editable
   🛠 Installed rust-query-engine-greatest-0.1.0
   ```

### Running Python Tests

Execute the Python test suite to validate the `GREATEST` function against Spark's implementation.

1. **Run the Tests:**

   ```bash
   python3 ./tests/greatest_test.py
   ```

   **Sample Output:**

   ```bash
   Setting default log level to "WARN".
   ...
   Starting Greatest Function Tests...

   Test 'test_greatest_int8': PASSED.
   Test 'test_greatest_int32': PASSED.
   Test 'test_greatest_float64_with_nan': PASSED.
   Test 'test_greatest_with_infinities': PASSED.
   Test 'test_greatest_date32': PASSED.
   Test 'test_greatest_date64': PASSED.
   ...
   ...
   All tests completed.
   ```

### Building and Testing DataFusion Functions

1. **Navigate to the DataFusion Functions Directory:**

   ```bash
   cd datafusion-greatest/
   ```

2. **Build and Run Tests for DataFusion Functions:**

   ```bash
   cargo build && cargo test -p datafusion-functions-nested@42.0.0
   ```

   **Sample Output:**

   ```bash
   Compiling datafusion-functions-nested v42.0.0 ...
   Finished test profile [unoptimized + debuginfo] target(s) in 9.54s
    Running unittests src/lib.rs (target/debug/deps/datafusion_functions_nested-cb97c815e57cf316)

    running 24 tests
    test expr_ext::tests::test_index ... ok
    ...
    test greatest::tests::test_greatest_code_generation_limit ... ok

    test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

    Doc-tests datafusion_functions_nested

    running 2 tests
    test datafusion/functions-nested/src/expr_ext.rs - expr_ext::SliceAccessor (line 66) ... ok
    test datafusion/functions-nested/src/expr_ext.rs - expr_ext::IndexAccessor (line 36) ... ok

    test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.48s
   ```

### Building and Using the DataFusion CLI

1. **Navigate to the DataFusion CLI Directory:**

   ```bash
   cd datafusion-cli/
   ```

2. **Build the CLI:**

   ```bash
   cargo build
   ```

3. **Run the CLI:**

   ```bash
   ./target/debug/datafusion-cli
   ```

   The CLI allows you to execute SQL scripts, including those that test the `GREATEST` function.

   **Sample SQL Usage:**

   ```sql
   -- Example SQL to test the GREATEST function
   WITH data(col1, col2, col3) AS (
       VALUES
           (1, 2, 7),
           (4, NULL, 5),
           (3, 6, NULL),
           (NULL, 8, 9)
   )
   SELECT GREATEST(col1, col2, col3) AS greatest_value FROM data;
   ```

## Usage

### Running the Rust Main

After building the Rust project, you can run the main executable to see sample outputs of the `GREATEST` function:

```bash
cargo run
```

### Using the Python Module

With the Python bindings installed, you can use the `GREATEST` function in your Python scripts:

```python
import greatest

# Example usage
result = greatest.run_greatest([[1, 4, 3, None], [2, None, 6, 8], [7, 5, None, 9]])
print(result)
# Output: [7, 5, 6, 9]
```

Contributions are welcome! Please follow these steps to contribute:

## License

This project is licensed under the [Apache License 2.0](LICENSE).

---
