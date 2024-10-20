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

//! [`SqlToRel`]: SQL Query Planner (produces [`LogicalPlan`] from SQL AST)
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use arrow_schema::*;
use datafusion_common::{
    field_not_found, internal_err, plan_datafusion_err, DFSchemaRef, SchemaError,
};
use sqlparser::ast::{ArrayElemTypeDef, ExactNumberInfo};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{DataType as SQLDataType, Ident, ObjectName, TableAlias};
use sqlparser::ast::{TimezoneInfo, Value};

use datafusion_common::TableReference;
use datafusion_common::{
    not_impl_err, plan_err, unqualified_field_not_found, DFSchema, DataFusionError,
    Result,
};
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::utils::find_column_exprs;
use datafusion_expr::{col, Expr};

use crate::utils::{make_decimal_type, value_to_string};
pub use datafusion_expr::planner::ContextProvider;

/// SQL parser options
#[derive(Debug)]
pub struct ParserOptions {
    pub parse_float_as_decimal: bool,
    pub enable_ident_normalization: bool,
    pub support_varchar_with_length: bool,
    pub enable_options_value_normalization: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            parse_float_as_decimal: false,
            enable_ident_normalization: true,
            support_varchar_with_length: true,
            enable_options_value_normalization: true,
        }
    }
}

/// Ident Normalizer
#[derive(Debug)]
pub struct IdentNormalizer {
    normalize: bool,
}

impl Default for IdentNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }

    pub fn normalize(&self, ident: Ident) -> String {
        if self.normalize {
            crate::utils::normalize_ident(ident)
        } else {
            ident.value
        }
    }
}

/// Value Normalizer
#[derive(Debug)]
pub struct ValueNormalizer {
    normalize: bool,
}

impl Default for ValueNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl ValueNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }

    pub fn normalize(&self, value: Value) -> Option<String> {
        match (value_to_string(&value), self.normalize) {
            (Some(s), true) => Some(s.to_ascii_lowercase()),
            (Some(s), false) => Some(s),
            (None, _) => None,
        }
    }
}

/// Struct to store the states used by the Planner. The Planner will leverage the states to resolve
/// CTEs, Views, subqueries and PREPARE statements. The states include
/// Common Table Expression (CTE) provided with WITH clause and
/// Parameter Data Types provided with PREPARE statement and the query schema of the
/// outer query plan.
///
/// # Cloning
///
/// Only the `ctes` are truly cloned when the `PlannerContext` is cloned. This helps resolve
/// scoping issues of CTEs. By using cloning, a subquery can inherit CTEs from the outer query
/// and can also define its own private CTEs without affecting the outer query.
///
#[derive(Debug, Clone)]
pub struct PlannerContext {
    /// Data types for numbered parameters ($1, $2, etc), if supplied
    /// in `PREPARE` statement
    prepare_param_data_types: Arc<Vec<DataType>>,
    /// Map of CTE name to logical plan of the WITH clause.
    /// Use `Arc<LogicalPlan>` to allow cheap cloning
    ctes: HashMap<String, Arc<LogicalPlan>>,
    /// The query schema of the outer query plan, used to resolve the columns in subquery
    outer_query_schema: Option<DFSchemaRef>,
    /// The joined schemas of all FROM clauses planned so far. When planning LATERAL
    /// FROM clauses, this should become a suffix of the `outer_query_schema`.
    outer_from_schema: Option<DFSchemaRef>,
}

impl Default for PlannerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerContext {
    /// Create an empty PlannerContext
    pub fn new() -> Self {
        Self {
            prepare_param_data_types: Arc::new(vec![]),
            ctes: HashMap::new(),
            outer_query_schema: None,
            outer_from_schema: None,
        }
    }

    /// Update the PlannerContext with provided prepare_param_data_types
    pub fn with_prepare_param_data_types(
        mut self,
        prepare_param_data_types: Vec<DataType>,
    ) -> Self {
        self.prepare_param_data_types = prepare_param_data_types.into();
        self
    }

    // Return a reference to the outer query's schema
    pub fn outer_query_schema(&self) -> Option<&DFSchema> {
        self.outer_query_schema.as_ref().map(|s| s.as_ref())
    }

    /// Sets the outer query schema, returning the existing one, if
    /// any
    pub fn set_outer_query_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.outer_query_schema, &mut schema);
        schema
    }

    // Return a clone of the outer FROM schema
    pub fn outer_from_schema(&self) -> Option<Arc<DFSchema>> {
        self.outer_from_schema.clone()
    }

    /// Sets the outer FROM schema, returning the existing one, if any
    pub fn set_outer_from_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.outer_from_schema, &mut schema);
        schema
    }

    /// Extends the FROM schema, returning the existing one, if any
    pub fn extend_outer_from_schema(&mut self, schema: &DFSchemaRef) -> Result<()> {
        match self.outer_from_schema.as_mut() {
            Some(from_schema) => Arc::make_mut(from_schema).merge(schema),
            None => self.outer_from_schema = Some(Arc::clone(schema)),
        };
        Ok(())
    }

    /// Return the types of parameters (`$1`, `$2`, etc) if known
    pub fn prepare_param_data_types(&self) -> &[DataType] {
        &self.prepare_param_data_types
    }

    /// Returns true if there is a Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn contains_cte(&self, cte_name: &str) -> bool {
        self.ctes.contains_key(cte_name)
    }

    /// Inserts a LogicalPlan for the Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn insert_cte(&mut self, cte_name: impl Into<String>, plan: LogicalPlan) {
        let cte_name = cte_name.into();
        self.ctes.insert(cte_name, Arc::new(plan));
    }

    /// Return a plan for the Common Table Expression (CTE) / Subquery for the
    /// specified name
    pub fn get_cte(&self, cte_name: &str) -> Option<&LogicalPlan> {
        self.ctes.get(cte_name).map(|cte| cte.as_ref())
    }

    /// Remove the plan of CTE / Subquery for the specified name
    pub(super) fn remove_cte(&mut self, cte_name: &str) {
        self.ctes.remove(cte_name);
    }
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    pub(crate) context_provider: &'a S,
    pub(crate) options: ParserOptions,
    pub(crate) ident_normalizer: IdentNormalizer,
    pub(crate) value_normalizer: ValueNormalizer,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(context_provider: &'a S) -> Self {
        Self::new_with_options(context_provider, ParserOptions::default())
    }

    /// Create a new query planner
    pub fn new_with_options(context_provider: &'a S, options: ParserOptions) -> Self {
        let ident_normalize = options.enable_ident_normalization;
        let options_value_normalize = options.enable_options_value_normalization;

        SqlToRel {
            context_provider,
            options,
            ident_normalizer: IdentNormalizer::new(ident_normalize),
            value_normalizer: ValueNormalizer::new(options_value_normalize),
        }
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_data_type(&column.data_type)?;
            let not_nullable = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::NotNull);
            fields.push(Field::new(
                self.ident_normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            ));
        }

        Ok(Schema::new(fields))
    }

    /// Returns a vector of (column_name, default_expr) pairs
    pub(super) fn build_column_defaults(
        &self,
        columns: &Vec<SQLColumnDef>,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<(String, Expr)>> {
        let mut column_defaults = vec![];
        // Default expressions are restricted, column references are not allowed
        let empty_schema = DFSchema::empty();
        let error_desc = |e: DataFusionError| match e {
            DataFusionError::SchemaError(SchemaError::FieldNotFound { .. }, _) => {
                plan_datafusion_err!(
                    "Column reference is not allowed in the DEFAULT expression : {}",
                    e
                )
            }
            _ => e,
        };

        for column in columns {
            if let Some(default_sql_expr) =
                column.options.iter().find_map(|o| match &o.option {
                    ColumnOption::Default(expr) => Some(expr),
                    _ => None,
                })
            {
                let default_expr = self
                    .sql_to_expr(default_sql_expr.clone(), &empty_schema, planner_context)
                    .map_err(error_desc)?;
                column_defaults.push((
                    self.ident_normalizer.normalize(column.name.clone()),
                    default_expr,
                ));
            }
        }
        Ok(column_defaults)
    }

    /// Apply the given TableAlias to the input plan
    pub(crate) fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let plan = self.apply_expr_alias(plan, alias.columns)?;

        LogicalPlanBuilder::from(plan)
            .alias(TableReference::bare(
                self.ident_normalizer.normalize(alias.name),
            ))?
            .build()
    }

    pub(crate) fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: Vec<Ident>,
    ) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            plan_err!(
                "Source table contains {} columns but only {} names given as column alias",
                plan.schema().fields().len(),
                idents.len()
            )
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(self.ident_normalizer.normalize(ident))
                }))?
                .build()
        }
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    pub(crate) fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(unqualified_field_not_found(col.name.as_str(), schema))
                        }
                    }
                }
                .map_err(|_: DataFusionError| {
                    field_not_found(col.relation.clone(), col.name.as_str(), schema)
                }),
                _ => internal_err!("Not a column"),
            })
    }

    pub(crate) fn convert_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type)) => {
                // Arrays may be multi-dimensional.
                let inner_data_type = self.convert_data_type(inner_sql_type)?;
                Ok(DataType::new_list(inner_data_type, true))
            }
            SQLDataType::Array(ArrayElemTypeDef::SquareBracket(
                inner_sql_type,
                maybe_array_size,
            )) => {
                let inner_data_type = self.convert_data_type(inner_sql_type)?;
                if let Some(array_size) = maybe_array_size {
                    Ok(DataType::new_fixed_size_list(
                        inner_data_type,
                        *array_size as i32,
                        true,
                    ))
                } else {
                    Ok(DataType::new_list(inner_data_type, true))
                }
            }
            SQLDataType::Array(ArrayElemTypeDef::None) => {
                not_impl_err!("Arrays with unspecified type is not supported")
            }
            other => self.convert_simple_data_type(other),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::Boolean | SQLDataType::Bool => Ok(DataType::Boolean),
            SQLDataType::TinyInt(_) => Ok(DataType::Int8),
            SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(DataType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => Ok(DataType::Int32),
            SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(DataType::Int64),
            SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
            SQLDataType::UnsignedSmallInt(_) | SQLDataType::UnsignedInt2(_) => Ok(DataType::UInt16),
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) | SQLDataType::UnsignedInt4(_) => {
                Ok(DataType::UInt32)
            }
            SQLDataType::Varchar(length) => {
                match (length, self.options.support_varchar_with_length) {
                    (Some(_), false) => plan_err!("does not support Varchar with length, please set `support_varchar_with_length` to be true"),
                    _ => Ok(DataType::Utf8),
                }
            }
            SQLDataType::UnsignedBigInt(_) | SQLDataType::UnsignedInt8(_) => Ok(DataType::UInt64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Float4 => Ok(DataType::Float32),
            SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => Ok(DataType::Float64),
            SQLDataType::Char(_)
            | SQLDataType::Text
            | SQLDataType::String(_) => Ok(DataType::Utf8),
            SQLDataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    self.context_provider.options().execution.time_zone.clone()
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(DataType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!(
                        "Unsupported SQL type {sql_type:?}"
                    )
                }
            }
            SQLDataType::Numeric(exact_number_info)
            | SQLDataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (None, None),
                    ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            SQLDataType::Bytea => Ok(DataType::Binary),
            SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
            SQLDataType::Struct(fields, _) => {
                let fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let data_type = self.convert_data_type(&field.field_type)?;
                        let field_name = match &field.field_name{
                            Some(ident) => ident.clone(),
                            None => Ident::new(format!("c{idx}"))
                        };
                        Ok(Arc::new(Field::new(
                            self.ident_normalizer.normalize(field_name),
                            data_type,
                            true,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(Fields::from(fields)))
            }
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/datafusion/issues/3059
            SQLDataType::Nvarchar(_)
            | SQLDataType::JSON
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Custom(_, _)
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            // Precision is not supported
            | SQLDataType::Timestamp(Some(_), _)
            // Precision is not supported
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_)
            | SQLDataType::Bytes(_)
            | SQLDataType::Int64
            | SQLDataType::Float64
            | SQLDataType::JSONB
            | SQLDataType::Unspecified
            // Clickhouse datatypes
            | SQLDataType::Int16
            | SQLDataType::Int32
            | SQLDataType::Int128
            | SQLDataType::Int256
            | SQLDataType::UInt8
            | SQLDataType::UInt16
            | SQLDataType::UInt32
            | SQLDataType::UInt64
            | SQLDataType::UInt128
            | SQLDataType::UInt256
            | SQLDataType::Float32
            | SQLDataType::Date32
            | SQLDataType::Datetime64(_, _)
            | SQLDataType::FixedString(_)
            | SQLDataType::Map(_, _)
            | SQLDataType::Tuple(_)
            | SQLDataType::Nested(_)
            | SQLDataType::Union(_)
            | SQLDataType::Nullable(_)
            | SQLDataType::LowCardinality(_)
            | SQLDataType::Trigger
            => not_impl_err!(
                "Unsupported SQL type {sql_type:?}"
            ),
        }
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName,
    ) -> Result<TableReference> {
        object_name_to_table_reference(
            object_name,
            self.options.enable_ident_normalization,
        )
    }
}

/// Create a [`TableReference`] after normalizing the specified ObjectName
///
/// Examples
/// ```text
/// ['foo']          -> Bare { table: "foo" }
/// ['"foo.bar"]]    -> Bare { table: "foo.bar" }
/// ['foo', 'Bar']   -> Partial { schema: "foo", table: "bar" } <-- note lower case "bar"
/// ['foo', 'bar']   -> Partial { schema: "foo", table: "bar" }
/// ['foo', '"Bar"'] -> Partial { schema: "foo", table: "Bar" }
/// ```
pub fn object_name_to_table_reference(
    object_name: ObjectName,
    enable_normalization: bool,
) -> Result<TableReference> {
    // Use destructure to make it clear no fields on ObjectName are ignored
    let ObjectName(idents) = object_name;
    idents_to_table_reference(idents, enable_normalization)
}

/// Create a [`TableReference`] after normalizing the specified identifier
pub(crate) fn idents_to_table_reference(
    idents: Vec<Ident>,
    enable_normalization: bool,
) -> Result<TableReference> {
    struct IdentTaker(Vec<Ident>);
    /// Take the next identifier from the back of idents, panic'ing if
    /// there are none left
    impl IdentTaker {
        fn take(&mut self, enable_normalization: bool) -> String {
            let ident = self.0.pop().expect("no more identifiers");
            IdentNormalizer::new(enable_normalization).normalize(ident)
        }
    }

    let mut taker = IdentTaker(idents);

    match taker.0.len() {
        1 => {
            let table = taker.take(enable_normalization);
            Ok(TableReference::bare(table))
        }
        2 => {
            let table = taker.take(enable_normalization);
            let schema = taker.take(enable_normalization);
            Ok(TableReference::partial(schema, table))
        }
        3 => {
            let table = taker.take(enable_normalization);
            let schema = taker.take(enable_normalization);
            let catalog = taker.take(enable_normalization);
            Ok(TableReference::full(catalog, schema, table))
        }
        _ => plan_err!("Unsupported compound identifier '{:?}'", taker.0),
    }
}

/// Construct a WHERE qualifier suitable for e.g. information_schema filtering
/// from the provided object identifiers (catalog, schema and table names).
pub fn object_name_to_qualifier(
    sql_table_name: &ObjectName,
    enable_normalization: bool,
) -> String {
    let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
    let normalizer = IdentNormalizer::new(enable_normalization);
    sql_table_name
        .0
        .iter()
        .rev()
        .zip(columns)
        .map(|(ident, column_name)| {
            format!(
                r#"{} = '{}'"#,
                column_name,
                normalizer.normalize(ident.clone())
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}