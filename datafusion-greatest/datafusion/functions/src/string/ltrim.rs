// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use std::any::Any;
use std::sync::OnceLock;

use crate::string::common::*;
use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::function::Hint;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

/// Returns the longest string  with leading characters removed. If the characters are not specified, whitespace is removed.
/// ltrim('zzzytest', 'xyz') = 'test'
fn ltrim<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let use_string_view = args[0].data_type() == &DataType::Utf8View;
    general_trim::<T>(args, TrimType::Left, use_string_view)
}

#[derive(Debug)]
pub struct LtrimFunc {
    signature: Signature,
}

impl Default for LtrimFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LtrimFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(1)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LtrimFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ltrim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == DataType::Utf8View {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "ltrim")
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => make_scalar_function(
                ltrim::<i32>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(args),
            DataType::LargeUtf8 => make_scalar_function(
                ltrim::<i64>,
                vec![Hint::Pad, Hint::AcceptsSingular],
            )(args),
            other => exec_err!(
                "Unsupported data type {other:?} for function ltrim,\
                expected Utf8, LargeUtf8 or Utf8View."
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_ltrim_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_ltrim_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Trims the specified trim string from the beginning of a string. If no trim string is provided, all whitespace is removed from the start of the input string.")
            .with_syntax_example("ltrim(str[, trim_str])")
            .with_sql_example(r#"```sql
> select ltrim('  datafusion  ');
+-------------------------------+
| ltrim(Utf8("  datafusion  ")) |
+-------------------------------+
| datafusion                    |
+-------------------------------+
> select ltrim('___datafusion___', '_');
+-------------------------------------------+
| ltrim(Utf8("___datafusion___"),Utf8("_")) |
+-------------------------------------------+
| datafusion___                             |
+-------------------------------------------+
```"#)
            .with_standard_argument("str", "String")
            .with_argument("trim_str", "String expression to trim from the beginning of the input string. Can be a constant, column, or function, and any combination of arithmetic operators. _Default is whitespace characters._")
            .with_related_udf("btrim")
            .with_related_udf("rtrim")
            .build()
            .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::ltrim::LtrimFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() {
        // String view cases for checking normal logic
        test_function!(
            LtrimFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("  alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("t")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabe"
                )))),
            ],
            Ok(Some("t")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        // Special string view case for checking unlined output(len > 12)
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "xxxalphabetalphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("x")))),
            ],
            Ok(Some("alphabetalphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        // String cases
        test_function!(
            LtrimFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("t")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabe")))),
            ],
            Ok(Some("t")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
    }
}