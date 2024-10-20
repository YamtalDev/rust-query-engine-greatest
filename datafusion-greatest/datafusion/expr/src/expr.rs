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

//! Logical Expressions: [`Expr`]

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter, Write};
use std::hash::{Hash, Hasher};
use std::mem;
use std::str::FromStr;
use std::sync::Arc;

use crate::expr_fn::binary_expr;
use crate::logical_plan::Subquery;
use crate::utils::expr_to_columns;
use crate::Volatility;
use crate::{
    built_in_window_function, udaf, BuiltInWindowFunction, ExprSchemable, Operator,
    Signature, WindowFrame, WindowUDF,
};

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    plan_err, Column, DFSchema, Result, ScalarValue, TableReference,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use sqlparser::ast::{
    display_comma_separated, ExceptSelectItem, ExcludeSelectItem, IlikeSelectItem,
    NullTreatment, RenameSelectItem, ReplaceSelectElement,
};

/// Represents logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
///
/// For example the expression `A + 1` will be represented as
///
///```text
///  BinaryExpr {
///    left: Expr::Column("A"),
///    op: Operator::Plus,
///    right: Expr::Literal(ScalarValue::Int32(Some(1)))
/// }
/// ```
///
/// # Creating Expressions
///
/// `Expr`s can be created directly, but it is often easier and less verbose to
/// use the fluent APIs in [`crate::expr_fn`] such as [`col`] and [`lit`], or
/// methods such as [`Expr::alias`], [`Expr::cast_to`], and [`Expr::Like`]).
///
/// See also [`ExprFunctionExt`] for creating aggregate and window functions.
///
/// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
///
/// # Schema Access
///
/// See [`ExprSchemable::get_type`] to access the [`DataType`] and nullability
/// of an `Expr`.
///
/// # Visiting and Rewriting `Expr`s
///
/// The `Expr` struct implements the [`TreeNode`] trait for walking and
/// rewriting expressions. For example [`TreeNode::apply`] recursively visits an
/// `Expr` and [`TreeNode::transform`] can be used to rewrite an expression. See
/// the examples below and [`TreeNode`] for more information.
///
/// # Examples
///
/// ## Column references and literals
///
/// [`Expr::Column`] refer to the values of columns and are often created with
/// the [`col`] function. For example to create an expression `c1` referring to
/// column named "c1":
///
/// [`col`]: crate::expr_fn::col
///
/// ```
/// # use datafusion_common::Column;
/// # use datafusion_expr::{lit, col, Expr};
/// let expr = col("c1");
/// assert_eq!(expr, Expr::Column(Column::from_name("c1")));
/// ```
///
/// [`Expr::Literal`] refer to literal, or constant, values. These are created
/// with the [`lit`] function. For example to create an expression `42`:
///
/// [`lit`]: crate::lit
///
/// ```
/// # use datafusion_common::{Column, ScalarValue};
/// # use datafusion_expr::{lit, col, Expr};
/// // All literals are strongly typed in DataFusion. To make an `i64` 42:
/// let expr = lit(42i64);
/// assert_eq!(expr, Expr::Literal(ScalarValue::Int64(Some(42))));
/// // To make a (typed) NULL:
/// let expr = Expr::Literal(ScalarValue::Int64(None));
/// // to make an (untyped) NULL (the optimizer will coerce this to the correct type):
/// let expr = lit(ScalarValue::Null);
/// ```
///
/// ## Binary Expressions
///
/// Exprs implement traits that allow easy to understand construction of more
/// complex expressions. For example, to create `c1 + c2` to add columns "c1" and
/// "c2" together
///
/// ```
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// // Use the `+` operator to add two columns together
/// let expr = col("c1") + col("c2");
/// assert!(matches!(expr, Expr::BinaryExpr { ..} ));
/// if let Expr::BinaryExpr(binary_expr) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   assert_eq!(*binary_expr.right, col("c2"));
///   assert_eq!(binary_expr.op, Operator::Plus);
/// }
/// ```
///
/// The expression `c1 = 42` to compares the value in column "c1" to the
/// literal value `42`:
///
/// ```
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// let expr = col("c1").eq(lit(42_i32));
/// assert!(matches!(expr, Expr::BinaryExpr { .. } ));
/// if let Expr::BinaryExpr(binary_expr) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   let scalar = ScalarValue::Int32(Some(42));
///   assert_eq!(*binary_expr.right, Expr::Literal(scalar));
///   assert_eq!(binary_expr.op, Operator::Eq);
/// }
/// ```
///
/// Here is how to implement the equivalent of `SELECT *` to select all
/// [`Expr::Column`] from a [`DFSchema`]'s columns:
///
/// ```
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::{DFSchema, Column};
/// # use datafusion_expr::Expr;
/// // Create a schema c1(int, c2 float)
/// let arrow_schema = Schema::new(vec![
///    Field::new("c1", DataType::Int32, false),
///    Field::new("c2", DataType::Float64, false),
/// ]);
/// // DFSchema is a an Arrow schema with optional relation name
/// let df_schema = DFSchema::try_from_qualified_schema("t1", &arrow_schema)
///   .unwrap();
///
/// // Form Vec<Expr> with an expression for each column in the schema
/// let exprs: Vec<_> = df_schema.iter()
///   .map(Expr::from)
///   .collect();
///
/// assert_eq!(exprs, vec![
///   Expr::from(Column::from_qualified_name("t1.c1")),
///   Expr::from(Column::from_qualified_name("t1.c2")),
/// ]);
/// ```
///
/// # Visiting and Rewriting `Expr`s
///
/// Here is an example that finds all literals in an `Expr` tree:
/// ```
/// # use std::collections::{HashSet};
/// use datafusion_common::ScalarValue;
/// # use datafusion_expr::{col, Expr, lit};
/// use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
/// // Expression a = 5 AND b = 6
/// let expr = col("a").eq(lit(5)) & col("b").eq(lit(6));
/// // find all literals in a HashMap
/// let mut scalars = HashSet::new();
/// // apply recursively visits all nodes in the expression tree
/// expr.apply(|e| {
///    if let Expr::Literal(scalar) = e {
///       scalars.insert(scalar);
///    }
///    // The return value controls whether to continue visiting the tree
///    Ok(TreeNodeRecursion::Continue)
/// }).unwrap();
/// // All subtrees have been visited and literals found
/// assert_eq!(scalars.len(), 2);
/// assert!(scalars.contains(&ScalarValue::Int32(Some(5))));
/// assert!(scalars.contains(&ScalarValue::Int32(Some(6))));
/// ```
///
/// Rewrite an expression, replacing references to column "a" in an
/// to the literal `42`:
///
///  ```
/// # use datafusion_common::tree_node::{Transformed, TreeNode};
/// # use datafusion_expr::{col, Expr, lit};
/// // expression a = 5 AND b = 6
/// let expr = col("a").eq(lit(5)).and(col("b").eq(lit(6)));
/// // rewrite all references to column "a" to the literal 42
/// let rewritten = expr.transform(|e| {
///   if let Expr::Column(c) = &e {
///     if &c.name == "a" {
///       // return Transformed::yes to indicate the node was changed
///       return Ok(Transformed::yes(lit(42)))
///     }
///   }
///   // return Transformed::no to indicate the node was not changed
///   Ok(Transformed::no(e))
/// }).unwrap();
/// // The expression has been rewritten
/// assert!(rewritten.transformed);
/// // to 42 = 5 AND b = 6
/// assert_eq!(rewritten.data, lit(42).eq(lit(5)).and(col("b").eq(lit(6))));
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Alias),
    /// A named reference to a qualified field in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(DataType, Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// LIKE expression
    Like(Like),
    /// LIKE expression that uses regular expressions
    SimilarTo(Like),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// True if argument is not NULL, false otherwise. This expression itself is never NULL.
    IsNotNull(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>),
    /// True if argument is true, false otherwise. This expression itself is never NULL.
    IsTrue(Box<Expr>),
    /// True if argument is  false, false otherwise. This expression itself is never NULL.
    IsFalse(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsUnknown(Box<Expr>),
    /// True if argument is FALSE or NULL, false otherwise. This expression itself is never NULL.
    IsNotTrue(Box<Expr>),
    /// True if argument is TRUE OR NULL, false otherwise. This expression itself is never NULL.
    IsNotFalse(Box<Expr>),
    /// True if argument is TRUE or FALSE, false otherwise. This expression itself is never NULL.
    IsNotUnknown(Box<Expr>),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>),
    /// Whether an expression is between a given range.
    Between(Between),
    /// The CASE expression is similar to a series of nested if/else and there are two forms that
    /// can be used. The first form consists of a series of boolean "when" expressions with
    /// corresponding "then" expressions, and an optional "else" expression.
    ///
    /// ```text
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    /// ```
    ///
    /// The second form uses a base expression and then a series of "when" clauses that match on a
    /// literal value.
    ///
    /// ```text
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    /// ```
    Case(Case),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast),
    /// Casts the expression to a given type and will return a null value if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    TryCast(TryCast),
    /// Represents the call of a scalar function with a set of arguments.
    ScalarFunction(ScalarFunction),
    /// Calls an aggregate function with arguments, and optional
    /// `ORDER BY`, `FILTER`, `DISTINCT` and `NULL TREATMENT`.
    ///
    /// See also [`ExprFunctionExt`] to set these fields.
    ///
    /// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
    AggregateFunction(AggregateFunction),
    /// Represents the call of a window function with arguments.
    WindowFunction(WindowFunction),
    /// Returns whether the list contains the expr value.
    InList(InList),
    /// EXISTS subquery
    Exists(Exists),
    /// IN subquery
    InSubquery(InSubquery),
    /// Scalar subquery
    ScalarSubquery(Subquery),
    /// Represents a reference to all available fields in a specific schema,
    /// with an optional (schema) qualifier.
    ///
    /// This expr has to be resolved to a list of columns before translating logical
    /// plan into physical plan.
    Wildcard {
        qualifier: Option<TableReference>,
        options: WildcardOptions,
    },
    /// List of grouping set expressions. Only valid in the context of an aggregate
    /// GROUP BY expression list
    GroupingSet(GroupingSet),
    /// A place holder for parameters in a prepared statement
    /// (e.g. `$foo` or `$1`)
    Placeholder(Placeholder),
    /// A place holder which hold a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterReferenceColumn(DataType, Column),
    /// Unnest expression
    Unnest(Unnest),
}

impl Default for Expr {
    fn default() -> Self {
        Expr::Literal(ScalarValue::Null)
    }
}

/// Create an [`Expr`] from a [`Column`]
impl From<Column> for Expr {
    fn from(value: Column) -> Self {
        Expr::Column(value)
    }
}

/// Create an [`Expr`] from an optional qualifier and a [`FieldRef`]. This is
/// useful for creating [`Expr`] from a [`DFSchema`].
///
/// See example on [`Expr`]
impl<'a> From<(Option<&'a TableReference>, &'a FieldRef)> for Expr {
    fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self {
        Expr::from(Column::from(value))
    }
}

/// UNNEST expression.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Unnest {
    pub expr: Box<Expr>,
}

impl Unnest {
    /// Create a new Unnest expression.
    pub fn new(expr: Expr) -> Self {
        Self {
            expr: Box::new(expr),
        }
    }

    /// Create a new Unnest expression.
    pub fn new_boxed(boxed: Box<Expr>) -> Self {
        Self { expr: boxed }
    }
}

/// Alias expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub relation: Option<TableReference>,
    pub name: String,
}

impl Alias {
    /// Create an alias with an optional schema/field qualifier.
    pub fn new(
        expr: Expr,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            expr: Box::new(expr),
            relation: relation.map(|r| r.into()),
            name: name.into(),
        }
    }
}

/// Binary expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    /// Create a new binary expression
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Put parentheses around child binary expressions so that we can see the difference
        // between `(a OR b) AND c` and `a OR (b AND c)`. We only insert parentheses when needed,
        // based on operator precedence. For example, `(a AND b) OR c` and `a AND b OR c` are
        // equivalent and the parentheses are not necessary.

        fn write_child(
            f: &mut Formatter<'_>,
            expr: &Expr,
            precedence: u8,
        ) -> fmt::Result {
            match expr {
                Expr::BinaryExpr(child) => {
                    let p = child.op.precedence();
                    if p == 0 || p < precedence {
                        write!(f, "({child})")?;
                    } else {
                        write!(f, "{child}")?;
                    }
                }
                _ => write!(f, "{expr}")?,
            }
            Ok(())
        }

        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

/// CASE expression
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Case {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub expr: Option<Box<Expr>>,
    /// One or more when/then expressions
    pub when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
    /// Optional "else" expression
    pub else_expr: Option<Box<Expr>>,
}

impl Case {
    /// Create a new Case expression
    pub fn new(
        expr: Option<Box<Expr>>,
        when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
        else_expr: Option<Box<Expr>>,
    ) -> Self {
        Self {
            expr,
            when_then_expr,
            else_expr,
        }
    }
}

/// LIKE expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
    /// Whether to ignore case on comparing
    pub case_insensitive: bool,
}

impl Like {
    /// Create a new Like expression
    pub fn new(
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
        case_insensitive: bool,
    ) -> Self {
        Self {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }
    }
}

/// BETWEEN expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Between {
    /// The value to compare
    pub expr: Box<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
    /// The low end of the range
    pub low: Box<Expr>,
    /// The high end of the range
    pub high: Box<Expr>,
}

impl Between {
    /// Create a new Between expression
    pub fn new(expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr>) -> Self {
        Self {
            expr,
            negated,
            low,
            high,
        }
    }
}

/// ScalarFunction expression invokes a built-in scalar function
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct ScalarFunction {
    /// The function
    pub func: Arc<crate::ScalarUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
}

impl ScalarFunction {
    // return the Function's name
    pub fn name(&self) -> &str {
        self.func.name()
    }
}

impl ScalarFunction {
    /// Create a new ScalarFunction expression with a user-defined function (UDF)
    pub fn new_udf(udf: Arc<crate::ScalarUDF>, args: Vec<Expr>) -> Self {
        Self { func: udf, args }
    }
}

/// Access a sub field of a nested type, such as `Field` or `List`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum GetFieldAccess {
    /// Named field, for example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// Single list index, for example: `list[i]`
    ListIndex { key: Box<Expr> },
    /// List stride, for example `list[i:j:k]`
    ListRange {
        start: Box<Expr>,
        stop: Box<Expr>,
        stride: Box<Expr>,
    },
}

/// Cast expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl Cast {
    /// Create a new Cast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

/// TryCast Expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct TryCast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl TryCast {
    /// Create a new TryCast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

/// SORT expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Sort {
    /// The expression to sort on
    pub expr: Expr,
    /// The direction of the sort
    pub asc: bool,
    /// Whether to put Nulls before all other data values
    pub nulls_first: bool,
}

impl Sort {
    /// Create a new Sort expression
    pub fn new(expr: Expr, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }

    /// Create a new Sort expression with the opposite sort direction
    pub fn reverse(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            asc: !self.asc,
            nulls_first: !self.nulls_first,
        }
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " ASC")?;
        } else {
            write!(f, " DESC")?;
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST")?;
        } else {
            write!(f, " NULLS LAST")?;
        }
        Ok(())
    }
}

/// Aggregate function
///
/// See also  [`ExprFunctionExt`] to set these fields on `Expr`
///
/// [`ExprFunctionExt`]: crate::expr_fn::ExprFunctionExt
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct AggregateFunction {
    /// Name of the function
    pub func: Arc<crate::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ordering
    pub order_by: Option<Vec<Sort>>,
    pub null_treatment: Option<NullTreatment>,
}

impl AggregateFunction {
    /// Create a new AggregateFunction expression with a user-defined function (UDF)
    pub fn new_udf(
        func: Arc<crate::AggregateUDF>,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Sort>>,
        null_treatment: Option<NullTreatment>,
    ) -> Self {
        Self {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        }
    }
}

/// WindowFunction
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
/// Defines which implementation of an aggregate function DataFusion should call.
pub enum WindowFunctionDefinition {
    /// A built in aggregate function that leverages an aggregate function
    /// A a built-in window function
    BuiltInWindowFunction(built_in_window_function::BuiltInWindowFunction),
    /// A user defined aggregate function
    AggregateUDF(Arc<crate::AggregateUDF>),
    /// A user defined aggregate function
    WindowUDF(Arc<crate::WindowUDF>),
}

impl WindowFunctionDefinition {
    /// Returns the datatype of the window function
    pub fn return_type(
        &self,
        input_expr_types: &[DataType],
        _input_expr_nullable: &[bool],
        display_name: &str,
    ) -> Result<DataType> {
        match self {
            WindowFunctionDefinition::BuiltInWindowFunction(fun) => {
                fun.return_type(input_expr_types)
            }
            WindowFunctionDefinition::AggregateUDF(fun) => {
                fun.return_type(input_expr_types)
            }
            WindowFunctionDefinition::WindowUDF(fun) => fun
                .field(WindowUDFFieldArgs::new(input_expr_types, display_name))
                .map(|field| field.data_type().clone()),
        }
    }

    /// The signatures supported by the function `fun`.
    pub fn signature(&self) -> Signature {
        match self {
            WindowFunctionDefinition::BuiltInWindowFunction(fun) => fun.signature(),
            WindowFunctionDefinition::AggregateUDF(fun) => fun.signature().clone(),
            WindowFunctionDefinition::WindowUDF(fun) => fun.signature().clone(),
        }
    }

    /// Function's name for display
    pub fn name(&self) -> &str {
        match self {
            WindowFunctionDefinition::BuiltInWindowFunction(fun) => fun.name(),
            WindowFunctionDefinition::WindowUDF(fun) => fun.name(),
            WindowFunctionDefinition::AggregateUDF(fun) => fun.name(),
        }
    }
}

impl fmt::Display for WindowFunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WindowFunctionDefinition::BuiltInWindowFunction(fun) => {
                std::fmt::Display::fmt(fun, f)
            }
            WindowFunctionDefinition::AggregateUDF(fun) => std::fmt::Display::fmt(fun, f),
            WindowFunctionDefinition::WindowUDF(fun) => std::fmt::Display::fmt(fun, f),
        }
    }
}

impl From<BuiltInWindowFunction> for WindowFunctionDefinition {
    fn from(value: BuiltInWindowFunction) -> Self {
        Self::BuiltInWindowFunction(value)
    }
}

impl From<Arc<crate::AggregateUDF>> for WindowFunctionDefinition {
    fn from(value: Arc<crate::AggregateUDF>) -> Self {
        Self::AggregateUDF(value)
    }
}

impl From<Arc<WindowUDF>> for WindowFunctionDefinition {
    fn from(value: Arc<WindowUDF>) -> Self {
        Self::WindowUDF(value)
    }
}

/// Window function
///
/// Holds the actual actual function to call [`WindowFunction`] as well as its
/// arguments (`args`) and the contents of the `OVER` clause:
///
/// 1. `PARTITION BY`
/// 2. `ORDER BY`
/// 3. Window frame (e.g. `ROWS 1 PRECEDING AND 1 FOLLOWING`)
///
/// # Example
/// ```
/// # use datafusion_expr::{Expr, BuiltInWindowFunction, col, ExprFunctionExt};
/// # use datafusion_expr::expr::WindowFunction;
/// // Create FIRST_VALUE(a) OVER (PARTITION BY b ORDER BY c)
/// let expr = Expr::WindowFunction(
///     WindowFunction::new(BuiltInWindowFunction::FirstValue, vec![col("a")])
/// )
///   .partition_by(vec![col("b")])
///   .order_by(vec![col("b").sort(true, true)])
///   .build()
///   .unwrap();
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct WindowFunction {
    /// Name of the function
    pub fun: WindowFunctionDefinition,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// List of partition by expressions
    pub partition_by: Vec<Expr>,
    /// List of order by expressions
    pub order_by: Vec<Sort>,
    /// Window frame
    pub window_frame: WindowFrame,
    /// Specifies how NULL value is treated: ignore or respect
    pub null_treatment: Option<NullTreatment>,
}

impl WindowFunction {
    /// Create a new Window expression with the specified argument an
    /// empty `OVER` clause
    pub fn new(fun: impl Into<WindowFunctionDefinition>, args: Vec<Expr>) -> Self {
        Self {
            fun: fun.into(),
            args,
            partition_by: Vec::default(),
            order_by: Vec::default(),
            window_frame: WindowFrame::new(None),
            null_treatment: None,
        }
    }
}

/// Find DataFusion's built-in window function by name.
pub fn find_df_window_func(name: &str) -> Option<WindowFunctionDefinition> {
    let name = name.to_lowercase();
    // Code paths for window functions leveraging ordinary aggregators and
    // built-in window functions are quite different, and the same function
    // may have different implementations for these cases. If the sought
    // function is not found among built-in window functions, we search for
    // it among aggregate functions.
    if let Ok(built_in_function) =
        built_in_window_function::BuiltInWindowFunction::from_str(name.as_str())
    {
        Some(WindowFunctionDefinition::BuiltInWindowFunction(
            built_in_function,
        ))
    } else {
        None
    }
}

/// EXISTS expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Exists {
    /// Subquery that will produce a single column of data
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl Exists {
    // Create a new Exists expression.
    pub fn new(subquery: Subquery, negated: bool) -> Self {
        Self { subquery, negated }
    }
}

/// User Defined Aggregate Function
///
/// See [`udaf::AggregateUDF`] for more information.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateUDF {
    /// The function
    pub fun: Arc<udaf::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ORDER BY applied prior to aggregating
    pub order_by: Option<Vec<Expr>>,
}

impl AggregateUDF {
    /// Create a new AggregateUDF expression
    pub fn new(
        fun: Arc<udaf::AggregateUDF>,
        args: Vec<Expr>,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            fun,
            args,
            filter,
            order_by,
        }
    }
}

/// InList expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InList {
    /// Create a new InList expression
    pub fn new(expr: Box<Expr>, list: Vec<Expr>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }
}

/// IN subquery
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct InSubquery {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// Subquery that will produce a single column of data to compare against
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InSubquery {
    /// Create a new InSubquery expression
    pub fn new(expr: Box<Expr>, subquery: Subquery, negated: bool) -> Self {
        Self {
            expr,
            subquery,
            negated,
        }
    }
}

/// Placeholder, representing bind parameter values such as `$1` or `$name`.
///
/// The type of these parameters is inferred using [`Expr::infer_placeholder_types`]
/// or can be specified directly using `PREPARE` statements.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Placeholder {
    /// The identifier of the parameter, including the leading `$` (e.g, `"$1"` or `"$foo"`)
    pub id: String,
    /// The type the parameter will be filled in with
    pub data_type: Option<DataType>,
}

impl Placeholder {
    /// Create a new Placeholder expression
    pub fn new(id: String, data_type: Option<DataType>) -> Self {
        Self { id, data_type }
    }
}

/// Grouping sets
///
/// See <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>
/// for Postgres definition.
/// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
/// for Apache Spark definition.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum GroupingSet {
    /// Rollup grouping sets
    Rollup(Vec<Expr>),
    /// Cube grouping sets
    Cube(Vec<Expr>),
    /// User-defined grouping sets
    GroupingSets(Vec<Vec<Expr>>),
}

impl GroupingSet {
    /// Return all distinct exprs in the grouping set. For `CUBE` and `ROLLUP` this
    /// is just the underlying list of exprs. For `GROUPING SET` we need to deduplicate
    /// the exprs in the underlying sets.
    pub fn distinct_expr(&self) -> Vec<&Expr> {
        match self {
            GroupingSet::Rollup(exprs) | GroupingSet::Cube(exprs) => {
                exprs.iter().collect()
            }
            GroupingSet::GroupingSets(groups) => {
                let mut exprs: Vec<&Expr> = vec![];
                for exp in groups.iter().flatten() {
                    if !exprs.contains(&exp) {
                        exprs.push(exp);
                    }
                }
                exprs
            }
        }
    }
}

/// Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug, Default)]
pub struct WildcardOptions {
    /// `[ILIKE...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub ilike: Option<IlikeSelectItem>,
    /// `[EXCLUDE...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub exclude: Option<ExcludeSelectItem>,
    /// `[EXCEPT...]`.
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_except>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#except>
    pub except: Option<ExceptSelectItem>,
    /// `[REPLACE]`
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#replace>
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub replace: Option<PlannedReplaceSelectItem>,
    /// `[RENAME ...]`.
    ///  Snowflake syntax: <https://docs.snowflake.com/en/sql-reference/sql/select#parameters>
    pub rename: Option<RenameSelectItem>,
}

impl WildcardOptions {
    pub fn with_replace(self, replace: PlannedReplaceSelectItem) -> Self {
        WildcardOptions {
            ilike: self.ilike,
            exclude: self.exclude,
            except: self.except,
            replace: Some(replace),
            rename: self.rename,
        }
    }
}

impl Display for WildcardOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(ilike) = &self.ilike {
            write!(f, " {ilike}")?;
        }
        if let Some(exclude) = &self.exclude {
            write!(f, " {exclude}")?;
        }
        if let Some(except) = &self.except {
            write!(f, " {except}")?;
        }
        if let Some(replace) = &self.replace {
            write!(f, " {replace}")?;
        }
        if let Some(rename) = &self.rename {
            write!(f, " {rename}")?;
        }
        Ok(())
    }
}

/// The planned expressions for `REPLACE`
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug, Default)]
pub struct PlannedReplaceSelectItem {
    /// The original ast nodes
    pub items: Vec<ReplaceSelectElement>,
    /// The expression planned from the ast nodes. They will be used when expanding the wildcard.
    pub planned_expressions: Vec<Expr>,
}

impl Display for PlannedReplaceSelectItem {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "REPLACE")?;
        write!(f, " ({})", display_comma_separated(&self.items))?;
        Ok(())
    }
}

impl PlannedReplaceSelectItem {
    pub fn items(&self) -> &[ReplaceSelectElement] {
        &self.items
    }

    pub fn expressions(&self) -> &[Expr] {
        &self.planned_expressions
    }
}

impl Expr {
    #[deprecated(since = "40.0.0", note = "use schema_name instead")]
    pub fn display_name(&self) -> Result<String> {
        Ok(self.schema_name().to_string())
    }

    /// The name of the column (field) that this `Expr` will produce.
    ///
    /// For example, for a projection (e.g. `SELECT <expr>`) the resulting arrow
    /// [`Schema`] will have a field with this name.
    ///
    /// Note that the resulting string is subtlety different from the `Display`
    /// representation for certain `Expr`. Some differences:
    ///
    /// 1. [`Expr::Alias`], which shows only the alias itself
    /// 2. [`Expr::Cast`] / [`Expr::TryCast`], which only displays the expression
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::{col, lit};
    /// let expr = col("foo").eq(lit(42));
    /// assert_eq!("foo = Int32(42)", expr.schema_name().to_string());
    ///
    /// let expr = col("foo").alias("bar").eq(lit(11));
    /// assert_eq!("bar = Int32(11)", expr.schema_name().to_string());
    /// ```
    ///
    /// [`Schema`]: arrow::datatypes::Schema
    pub fn schema_name(&self) -> impl Display + '_ {
        SchemaDisplay(self)
    }

    /// Returns the qualifier and the schema name of this expression.
    ///
    /// Used when the expression forms the output field of a certain plan.
    /// The result is the field's qualifier and field name in the plan's
    /// output schema. We can use this qualified name to reference the field.
    pub fn qualified_name(&self) -> (Option<TableReference>, String) {
        match self {
            Expr::Column(Column { relation, name }) => (relation.clone(), name.clone()),
            Expr::Alias(Alias { relation, name, .. }) => (relation.clone(), name.clone()),
            _ => (None, self.schema_name().to_string()),
        }
    }

    /// Returns a full and complete string representation of this expression.
    #[deprecated(note = "use format! instead")]
    pub fn canonical_name(&self) -> String {
        format!("{self}")
    }

    /// Return String representation of the variant represented by `self`
    /// Useful for non-rust based bindings
    pub fn variant_name(&self) -> &str {
        match self {
            Expr::AggregateFunction { .. } => "AggregateFunction",
            Expr::Alias(..) => "Alias",
            Expr::Between { .. } => "Between",
            Expr::BinaryExpr { .. } => "BinaryExpr",
            Expr::Case { .. } => "Case",
            Expr::Cast { .. } => "Cast",
            Expr::Column(..) => "Column",
            Expr::OuterReferenceColumn(_, _) => "Outer",
            Expr::Exists { .. } => "Exists",
            Expr::GroupingSet(..) => "GroupingSet",
            Expr::InList { .. } => "InList",
            Expr::InSubquery(..) => "InSubquery",
            Expr::IsNotNull(..) => "IsNotNull",
            Expr::IsNull(..) => "IsNull",
            Expr::Like { .. } => "Like",
            Expr::SimilarTo { .. } => "RLike",
            Expr::IsTrue(..) => "IsTrue",
            Expr::IsFalse(..) => "IsFalse",
            Expr::IsUnknown(..) => "IsUnknown",
            Expr::IsNotTrue(..) => "IsNotTrue",
            Expr::IsNotFalse(..) => "IsNotFalse",
            Expr::IsNotUnknown(..) => "IsNotUnknown",
            Expr::Literal(..) => "Literal",
            Expr::Negative(..) => "Negative",
            Expr::Not(..) => "Not",
            Expr::Placeholder(_) => "Placeholder",
            Expr::ScalarFunction(..) => "ScalarFunction",
            Expr::ScalarSubquery { .. } => "ScalarSubquery",
            Expr::ScalarVariable(..) => "ScalarVariable",
            Expr::TryCast { .. } => "TryCast",
            Expr::WindowFunction { .. } => "WindowFunction",
            Expr::Wildcard { .. } => "Wildcard",
            Expr::Unnest { .. } => "Unnest",
        }
    }

    /// Return `self == other`
    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Eq, other)
    }

    /// Return `self != other`
    pub fn not_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::NotEq, other)
    }

    /// Return `self > other`
    pub fn gt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Gt, other)
    }

    /// Return `self >= other`
    pub fn gt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GtEq, other)
    }

    /// Return `self < other`
    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Lt, other)
    }

    /// Return `self <= other`
    pub fn lt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LtEq, other)
    }

    /// Return `self && other`
    pub fn and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::And, other)
    }

    /// Return `self || other`
    pub fn or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Or, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            true,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self ILIKE other`
    pub fn ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            true,
        ))
    }

    /// Return `self NOT ILIKE other`
    pub fn not_ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(true, Box::new(self), Box::new(other), None, true))
    }

    /// Return the name to use for the specific Expr
    pub fn name_for_alias(&self) -> Result<String> {
        Ok(self.schema_name().to_string())
    }

    /// Ensure `expr` has the name as `original_name` by adding an
    /// alias if necessary.
    pub fn alias_if_changed(self, original_name: String) -> Result<Expr> {
        let new_name = self.name_for_alias()?;
        if new_name == original_name {
            return Ok(self);
        }

        Ok(self.alias(original_name))
    }

    /// Return `self AS name` alias expression
    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias(Alias::new(self, None::<&str>, name.into()))
    }

    /// Return `self AS name` alias expression with a specific qualifier
    pub fn alias_qualified(
        self,
        relation: Option<impl Into<TableReference>>,
        name: impl Into<String>,
    ) -> Expr {
        Expr::Alias(Alias::new(self, relation, name.into()))
    }

    /// Remove an alias from an expression if one exists.
    ///
    /// If the expression is not an alias, the expression is returned unchanged.
    /// This method does not remove aliases from nested expressions.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// // `foo as "bar"` is unaliased to `foo`
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.unalias(), col("foo"));
    ///
    /// // `foo as "bar" + baz` is not unaliased
    /// let expr = col("foo").alias("bar") + col("baz");
    /// assert_eq!(expr.clone().unalias(), expr);
    ///
    /// // `foo as "bar" as "baz" is unalaised to foo as "bar"
    /// let expr = col("foo").alias("bar").alias("baz");
    /// assert_eq!(expr.unalias(), col("foo").alias("bar"));
    /// ```
    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(alias) => *alias.expr,
            _ => self,
        }
    }

    /// Recursively removed potentially multiple aliases from an expression.
    ///
    /// This method removes nested aliases and returns [`Transformed`]
    /// to signal if the expression was changed.
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::col;
    /// // `foo as "bar"` is unaliased to `foo`
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.unalias_nested().data, col("foo"));
    ///
    /// // `foo as "bar" + baz` is  unaliased
    /// let expr = col("foo").alias("bar") + col("baz");
    /// assert_eq!(expr.clone().unalias_nested().data, col("foo") + col("baz"));
    ///
    /// // `foo as "bar" as "baz" is unalaised to foo
    /// let expr = col("foo").alias("bar").alias("baz");
    /// assert_eq!(expr.unalias_nested().data, col("foo"));
    /// ```
    pub fn unalias_nested(self) -> Transformed<Expr> {
        self.transform_down_up(
            |expr| {
                // f_down: skip subqueries.  Check in f_down to avoid recursing into them
                let recursion = if matches!(
                    expr,
                    Expr::Exists { .. } | Expr::ScalarSubquery(_) | Expr::InSubquery(_)
                ) {
                    // Subqueries could contain aliases so don't recurse into those
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                };
                Ok(Transformed::new(expr, false, recursion))
            },
            |expr| {
                // f_up: unalias on up so we can remove nested aliases like
                // `(x as foo) as bar`
                if let Expr::Alias(Alias { expr, .. }) = expr {
                    Ok(Transformed::yes(*expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            },
        )
        // Unreachable code: internal closure doesn't return err
        .unwrap()
    }

    /// Return `self IN <list>` if `negated` is false, otherwise
    /// return `self NOT IN <list>`.a
    pub fn in_list(self, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::InList(InList::new(Box::new(self), list, negated))
    }

    /// Return `IsNull(Box(self))
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    /// Create a sort configuration from an existing expression.
    ///
    /// ```
    /// # use datafusion_expr::col;
    /// let sort_expr = col("foo").sort(true, true); // SORT ASC NULLS_FIRST
    /// ```
    pub fn sort(self, asc: bool, nulls_first: bool) -> Sort {
        Sort::new(self, asc, nulls_first)
    }

    /// Return `IsTrue(Box(self))`
    pub fn is_true(self) -> Expr {
        Expr::IsTrue(Box::new(self))
    }

    /// Return `IsNotTrue(Box(self))`
    pub fn is_not_true(self) -> Expr {
        Expr::IsNotTrue(Box::new(self))
    }

    /// Return `IsFalse(Box(self))`
    pub fn is_false(self) -> Expr {
        Expr::IsFalse(Box::new(self))
    }

    /// Return `IsNotFalse(Box(self))`
    pub fn is_not_false(self) -> Expr {
        Expr::IsNotFalse(Box::new(self))
    }

    /// Return `IsUnknown(Box(self))`
    pub fn is_unknown(self) -> Expr {
        Expr::IsUnknown(Box::new(self))
    }

    /// Return `IsNotUnknown(Box(self))`
    pub fn is_not_unknown(self) -> Expr {
        Expr::IsNotUnknown(Box::new(self))
    }

    /// return `self BETWEEN low AND high`
    pub fn between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            false,
            Box::new(low),
            Box::new(high),
        ))
    }

    /// Return `self NOT BETWEEN low AND high`
    pub fn not_between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            true,
            Box::new(low),
            Box::new(high),
        ))
    }

    #[deprecated(since = "39.0.0", note = "use try_as_col instead")]
    pub fn try_into_col(&self) -> Result<Column> {
        match self {
            Expr::Column(it) => Ok(it.clone()),
            _ => plan_err!("Could not coerce '{self}' into Column!"),
        }
    }

    /// Return a reference to the inner `Column` if any
    ///
    /// returns `None` if the expression is not a `Column`
    ///
    /// Note: None may be returned for expressions that are not `Column` but
    /// are convertible to `Column` such as `Cast` expressions.
    ///
    /// Example
    /// ```
    /// # use datafusion_common::Column;
    /// use datafusion_expr::{col, Expr};
    /// let expr = col("foo");
    /// assert_eq!(expr.try_as_col(), Some(&Column::from("foo")));
    ///
    /// let expr = col("foo").alias("bar");
    /// assert_eq!(expr.try_as_col(), None);
    /// ```
    pub fn try_as_col(&self) -> Option<&Column> {
        if let Expr::Column(it) = self {
            Some(it)
        } else {
            None
        }
    }

    /// Returns the inner `Column` if any. This is a specialized version of
    /// [`Self::try_as_col`] that take Cast expressions into account when the
    /// expression is as on condition for joins.
    ///
    /// Called this method when you are sure that the expression is a `Column`
    /// or a `Cast` expression that wraps a `Column`.
    pub fn get_as_join_column(&self) -> Option<&Column> {
        match self {
            Expr::Column(c) => Some(c),
            Expr::Cast(Cast { expr, .. }) => match &**expr {
                Expr::Column(c) => Some(c),
                _ => None,
            },
            _ => None,
        }
    }

    /// Return all referenced columns of this expression.
    #[deprecated(since = "40.0.0", note = "use Expr::column_refs instead")]
    pub fn to_columns(&self) -> Result<HashSet<Column>> {
        let mut using_columns = HashSet::new();
        expr_to_columns(self, &mut using_columns)?;

        Ok(using_columns)
    }

    /// Return all references to columns in this expression.
    ///
    /// # Example
    /// ```
    /// # use std::collections::HashSet;
    /// # use datafusion_common::Column;
    /// # use datafusion_expr::col;
    /// // For an expression `a + (b * a)`
    /// let expr = col("a") + (col("b") * col("a"));
    /// let refs = expr.column_refs();
    /// // refs contains "a" and "b"
    /// assert_eq!(refs.len(), 2);
    /// assert!(refs.contains(&Column::new_unqualified("a")));
    /// assert!(refs.contains(&Column::new_unqualified("b")));
    /// ```
    pub fn column_refs(&self) -> HashSet<&Column> {
        let mut using_columns = HashSet::new();
        self.add_column_refs(&mut using_columns);
        using_columns
    }

    /// Adds references to all columns in this expression to the set
    ///
    /// See [`Self::column_refs`] for details
    pub fn add_column_refs<'a>(&'a self, set: &mut HashSet<&'a Column>) {
        self.apply(|expr| {
            if let Expr::Column(col) = expr {
                set.insert(col);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
    }

    /// Return all references to columns and their occurrence counts in the expression.
    ///
    /// # Example
    /// ```
    /// # use std::collections::HashMap;
    /// # use datafusion_common::Column;
    /// # use datafusion_expr::col;
    /// // For an expression `a + (b * a)`
    /// let expr = col("a") + (col("b") * col("a"));
    /// let mut refs = expr.column_refs_counts();
    /// // refs contains "a" and "b"
    /// assert_eq!(refs.len(), 2);
    /// assert_eq!(*refs.get(&Column::new_unqualified("a")).unwrap(), 2);
    /// assert_eq!(*refs.get(&Column::new_unqualified("b")).unwrap(), 1);
    /// ```
    pub fn column_refs_counts(&self) -> HashMap<&Column, usize> {
        let mut map = HashMap::new();
        self.add_column_ref_counts(&mut map);
        map
    }

    /// Adds references to all columns and their occurrence counts in the expression to
    /// the map.
    ///
    /// See [`Self::column_refs_counts`] for details
    pub fn add_column_ref_counts<'a>(&'a self, map: &mut HashMap<&'a Column, usize>) {
        self.apply(|expr| {
            if let Expr::Column(col) = expr {
                *map.entry(col).or_default() += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
    }

    /// Returns true if there are any column references in this Expr
    pub fn any_column_refs(&self) -> bool {
        self.exists(|expr| Ok(matches!(expr, Expr::Column(_))))
            .unwrap()
    }

    /// Return true when the expression contains out reference(correlated) expressions.
    pub fn contains_outer(&self) -> bool {
        self.exists(|expr| Ok(matches!(expr, Expr::OuterReferenceColumn { .. })))
            .unwrap()
    }

    /// Returns true if the expression node is volatile, i.e. whether it can return
    /// different results when evaluated multiple times with the same input.
    /// Note: unlike [`Self::is_volatile`], this function does not consider inputs:
    /// - `rand()` returns `true`,
    /// - `a + rand()` returns `false`
    pub fn is_volatile_node(&self) -> bool {
        matches!(self, Expr::ScalarFunction(func) if func.func.signature().volatility == Volatility::Volatile)
    }

    /// Returns true if the expression is volatile, i.e. whether it can return different
    /// results when evaluated multiple times with the same input.
    pub fn is_volatile(&self) -> Result<bool> {
        self.exists(|expr| Ok(expr.is_volatile_node()))
    }

    /// Recursively find all [`Expr::Placeholder`] expressions, and
    /// to infer their [`DataType`] from the context of their use.
    ///
    /// For example, gicen an expression like `<int32> = $0` will infer `$0` to
    /// have type `int32`.
    pub fn infer_placeholder_types(self, schema: &DFSchema) -> Result<Expr> {
        self.transform(|mut expr| {
            // Default to assuming the arguments are the same type
            if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = &mut expr {
                rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
                rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
            };
            if let Expr::Between(Between {
                expr,
                negated: _,
                low,
                high,
            }) = &mut expr
            {
                rewrite_placeholder(low.as_mut(), expr.as_ref(), schema)?;
                rewrite_placeholder(high.as_mut(), expr.as_ref(), schema)?;
            }
            Ok(Transformed::yes(expr))
        })
        .data()
    }

    /// Returns true if some of this `exprs` subexpressions may not be evaluated
    /// and thus any side effects (like divide by zero) may not be encountered
    pub fn short_circuits(&self) -> bool {
        match self {
            Expr::ScalarFunction(ScalarFunction { func, .. }) => func.short_circuits(),
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                matches!(op, Operator::And | Operator::Or)
            }
            Expr::Case { .. } => true,
            // Use explicit pattern match instead of a default
            // implementation, so that in the future if someone adds
            // new Expr types, they will check here as well
            Expr::AggregateFunction(..)
            | Expr::Alias(..)
            | Expr::Between(..)
            | Expr::Cast(..)
            | Expr::Column(..)
            | Expr::Exists(..)
            | Expr::GroupingSet(..)
            | Expr::InList(..)
            | Expr::InSubquery(..)
            | Expr::IsFalse(..)
            | Expr::IsNotFalse(..)
            | Expr::IsNotNull(..)
            | Expr::IsNotTrue(..)
            | Expr::IsNotUnknown(..)
            | Expr::IsNull(..)
            | Expr::IsTrue(..)
            | Expr::IsUnknown(..)
            | Expr::Like(..)
            | Expr::ScalarSubquery(..)
            | Expr::ScalarVariable(_, _)
            | Expr::SimilarTo(..)
            | Expr::Not(..)
            | Expr::Negative(..)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::TryCast(..)
            | Expr::Unnest(..)
            | Expr::Wildcard { .. }
            | Expr::WindowFunction(..)
            | Expr::Literal(..)
            | Expr::Placeholder(..) => false,
        }
    }

    /// Hashes the direct content of an `Expr` without recursing into its children.
    ///
    /// This method is useful to incrementally compute hashes, such as  in
    /// `CommonSubexprEliminate` which builds a deep hash of a node and its descendants
    /// during the bottom-up phase of the first traversal and so avoid computing the hash
    /// of the node and then the hash of its descendants separately.
    ///
    /// If a node doesn't have any children then this method is similar to `.hash()`, but
    /// not necessarily returns the same value.
    ///
    /// As it is pretty easy to forget changing this method when `Expr` changes the
    /// implementation doesn't use wildcard patterns (`..`, `_`) to catch changes
    /// compile time.
    pub fn hash_node<H: Hasher>(&self, hasher: &mut H) {
        mem::discriminant(self).hash(hasher);
        match self {
            Expr::Alias(Alias {
                expr: _expr,
                relation,
                name,
            }) => {
                relation.hash(hasher);
                name.hash(hasher);
            }
            Expr::Column(column) => {
                column.hash(hasher);
            }
            Expr::ScalarVariable(data_type, name) => {
                data_type.hash(hasher);
                name.hash(hasher);
            }
            Expr::Literal(scalar_value) => {
                scalar_value.hash(hasher);
            }
            Expr::BinaryExpr(BinaryExpr {
                left: _left,
                op,
                right: _right,
            }) => {
                op.hash(hasher);
            }
            Expr::Like(Like {
                negated,
                expr: _expr,
                pattern: _pattern,
                escape_char,
                case_insensitive,
            })
            | Expr::SimilarTo(Like {
                negated,
                expr: _expr,
                pattern: _pattern,
                escape_char,
                case_insensitive,
            }) => {
                negated.hash(hasher);
                escape_char.hash(hasher);
                case_insensitive.hash(hasher);
            }
            Expr::Not(_expr)
            | Expr::IsNotNull(_expr)
            | Expr::IsNull(_expr)
            | Expr::IsTrue(_expr)
            | Expr::IsFalse(_expr)
            | Expr::IsUnknown(_expr)
            | Expr::IsNotTrue(_expr)
            | Expr::IsNotFalse(_expr)
            | Expr::IsNotUnknown(_expr)
            | Expr::Negative(_expr) => {}
            Expr::Between(Between {
                expr: _expr,
                negated,
                low: _low,
                high: _high,
            }) => {
                negated.hash(hasher);
            }
            Expr::Case(Case {
                expr: _expr,
                when_then_expr: _when_then_expr,
                else_expr: _else_expr,
            }) => {}
            Expr::Cast(Cast {
                expr: _expr,
                data_type,
            })
            | Expr::TryCast(TryCast {
                expr: _expr,
                data_type,
            }) => {
                data_type.hash(hasher);
            }
            Expr::ScalarFunction(ScalarFunction { func, args: _args }) => {
                func.hash(hasher);
            }
            Expr::AggregateFunction(AggregateFunction {
                func,
                args: _args,
                distinct,
                filter: _filter,
                order_by: _order_by,
                null_treatment,
            }) => {
                func.hash(hasher);
                distinct.hash(hasher);
                null_treatment.hash(hasher);
            }
            Expr::WindowFunction(WindowFunction {
                fun,
                args: _args,
                partition_by: _partition_by,
                order_by: _order_by,
                window_frame,
                null_treatment,
            }) => {
                fun.hash(hasher);
                window_frame.hash(hasher);
                null_treatment.hash(hasher);
            }
            Expr::InList(InList {
                expr: _expr,
                list: _list,
                negated,
            }) => {
                negated.hash(hasher);
            }
            Expr::Exists(Exists { subquery, negated }) => {
                subquery.hash(hasher);
                negated.hash(hasher);
            }
            Expr::InSubquery(InSubquery {
                expr: _expr,
                subquery,
                negated,
            }) => {
                subquery.hash(hasher);
                negated.hash(hasher);
            }
            Expr::ScalarSubquery(subquery) => {
                subquery.hash(hasher);
            }
            Expr::Wildcard { qualifier, options } => {
                qualifier.hash(hasher);
                options.hash(hasher);
            }
            Expr::GroupingSet(grouping_set) => {
                mem::discriminant(grouping_set).hash(hasher);
                match grouping_set {
                    GroupingSet::Rollup(_exprs) | GroupingSet::Cube(_exprs) => {}
                    GroupingSet::GroupingSets(_exprs) => {}
                }
            }
            Expr::Placeholder(place_holder) => {
                place_holder.hash(hasher);
            }
            Expr::OuterReferenceColumn(data_type, column) => {
                data_type.hash(hasher);
                column.hash(hasher);
            }
            Expr::Unnest(Unnest { expr: _expr }) => {}
        };
    }
}

// Modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }) = expr {
        if data_type.is_none() {
            let other_dt = other.get_type(schema);
            match other_dt {
                Err(e) => {
                    Err(e.context(format!(
                        "Can not find type of {other} needed to infer type of {expr}"
                    )))?;
                }
                Ok(dt) => {
                    *data_type = Some(dt);
                }
            }
        };
    }
    Ok(())
}

#[macro_export]
macro_rules! expr_vec_fmt {
    ( $ARRAY:expr ) => {{
        $ARRAY
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ")
    }};
}

struct SchemaDisplay<'a>(&'a Expr);
impl<'a> Display for SchemaDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            // The same as Display
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::ScalarVariable(..)
            | Expr::OuterReferenceColumn(..)
            | Expr::Placeholder(_)
            | Expr::Wildcard { .. } => write!(f, "{}", self.0),

            Expr::AggregateFunction(AggregateFunction {
                func,
                args,
                distinct,
                filter,
                order_by,
                null_treatment,
            }) => {
                write!(
                    f,
                    "{}({}{})",
                    func.name(),
                    if *distinct { "DISTINCT " } else { "" },
                    schema_name_from_exprs_comma_seperated_without_space(args)?
                )?;

                if let Some(null_treatment) = null_treatment {
                    write!(f, " {}", null_treatment)?;
                }

                if let Some(filter) = filter {
                    write!(f, " FILTER (WHERE {filter})")?;
                };

                if let Some(order_by) = order_by {
                    write!(f, " ORDER BY [{}]", schema_name_from_sorts(order_by)?)?;
                };

                Ok(())
            }
            // Expr is not shown since it is aliased
            Expr::Alias(Alias { name, .. }) => write!(f, "{name}"),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(
                        f,
                        "{} NOT BETWEEN {} AND {}",
                        SchemaDisplay(expr),
                        SchemaDisplay(low),
                        SchemaDisplay(high),
                    )
                } else {
                    write!(
                        f,
                        "{} BETWEEN {} AND {}",
                        SchemaDisplay(expr),
                        SchemaDisplay(low),
                        SchemaDisplay(high),
                    )
                }
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                write!(f, "{} {op} {}", SchemaDisplay(left), SchemaDisplay(right),)
            }
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                write!(f, "CASE ")?;

                if let Some(e) = expr {
                    write!(f, "{} ", SchemaDisplay(e))?;
                }

                for (when, then) in when_then_expr {
                    write!(
                        f,
                        "WHEN {} THEN {} ",
                        SchemaDisplay(when),
                        SchemaDisplay(then),
                    )?;
                }

                if let Some(e) = else_expr {
                    write!(f, "ELSE {} ", SchemaDisplay(e))?;
                }

                write!(f, "END")
            }
            // Cast expr is not shown to be consistant with Postgres and Spark <https://github.com/apache/datafusion/pull/3222>
            Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
                write!(f, "{}", SchemaDisplay(expr))
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let inlist_name = schema_name_from_exprs(list)?;

                if *negated {
                    write!(f, "{} NOT IN {}", SchemaDisplay(expr), inlist_name)
                } else {
                    write!(f, "{} IN {}", SchemaDisplay(expr), inlist_name)
                }
            }
            Expr::Exists(Exists { negated: true, .. }) => write!(f, "NOT EXISTS"),
            Expr::Exists(Exists { negated: false, .. }) => write!(f, "EXISTS"),
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                write!(f, "GROUPING SETS (")?;
                for exprs in lists_of_exprs.iter() {
                    write!(f, "({})", schema_name_from_exprs(exprs)?)?;
                }
                write!(f, ")")
            }
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                write!(f, "ROLLUP ({})", schema_name_from_exprs(exprs)?)
            }
            Expr::IsNull(expr) => write!(f, "{} IS NULL", SchemaDisplay(expr)),
            Expr::IsNotNull(expr) => {
                write!(f, "{} IS NOT NULL", SchemaDisplay(expr))
            }
            Expr::IsUnknown(expr) => {
                write!(f, "{} IS UNKNOWN", SchemaDisplay(expr))
            }
            Expr::IsNotUnknown(expr) => {
                write!(f, "{} IS NOT UNKNOWN", SchemaDisplay(expr))
            }
            Expr::InSubquery(InSubquery { negated: true, .. }) => {
                write!(f, "NOT IN")
            }
            Expr::InSubquery(InSubquery { negated: false, .. }) => write!(f, "IN"),
            Expr::IsTrue(expr) => write!(f, "{} IS TRUE", SchemaDisplay(expr)),
            Expr::IsFalse(expr) => write!(f, "{} IS FALSE", SchemaDisplay(expr)),
            Expr::IsNotTrue(expr) => {
                write!(f, "{} IS NOT TRUE", SchemaDisplay(expr))
            }
            Expr::IsNotFalse(expr) => {
                write!(f, "{} IS NOT FALSE", SchemaDisplay(expr))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                write!(
                    f,
                    "{} {}{} {}",
                    SchemaDisplay(expr),
                    if *negated { "NOT " } else { "" },
                    if *case_insensitive { "ILIKE" } else { "LIKE" },
                    SchemaDisplay(pattern),
                )?;

                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::Negative(expr) => write!(f, "(- {})", SchemaDisplay(expr)),
            Expr::Not(expr) => write!(f, "NOT {}", SchemaDisplay(expr)),
            Expr::Unnest(Unnest { expr }) => {
                write!(f, "UNNEST({})", SchemaDisplay(expr))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                match func.schema_name(args) {
                    Ok(name) => {
                        write!(f, "{name}")
                    }
                    Err(e) => {
                        write!(f, "got error from schema_name {}", e)
                    }
                }
            }
            Expr::ScalarSubquery(Subquery { subquery, .. }) => {
                write!(f, "{}", subquery.schema().field(0).name())
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            }) => {
                write!(
                    f,
                    "{} {} {}",
                    SchemaDisplay(expr),
                    if *negated {
                        "NOT SIMILAR TO"
                    } else {
                        "SIMILAR TO"
                    },
                    SchemaDisplay(pattern),
                )?;
                if let Some(char) = escape_char {
                    write!(f, " CHAR '{char}'")?;
                }

                Ok(())
            }
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            }) => {
                write!(
                    f,
                    "{}({})",
                    fun,
                    schema_name_from_exprs_comma_seperated_without_space(args)?
                )?;

                if let Some(null_treatment) = null_treatment {
                    write!(f, " {}", null_treatment)?;
                }

                if !partition_by.is_empty() {
                    write!(
                        f,
                        " PARTITION BY [{}]",
                        schema_name_from_exprs(partition_by)?
                    )?;
                }

                if !order_by.is_empty() {
                    write!(f, " ORDER BY [{}]", schema_name_from_sorts(order_by)?)?;
                };

                write!(f, " {window_frame}")
            }
        }
    }
}

/// Get schema_name for Vector of expressions
///
/// Internal usage. Please call `schema_name_from_exprs` instead
// TODO: Use ", " to standardize the formatting of Vec<Expr>,
// <https://github.com/apache/datafusion/issues/10364>
pub(crate) fn schema_name_from_exprs_comma_seperated_without_space(
    exprs: &[Expr],
) -> Result<String, fmt::Error> {
    schema_name_from_exprs_inner(exprs, ",")
}

/// Get schema_name for Vector of expressions
pub fn schema_name_from_exprs(exprs: &[Expr]) -> Result<String, fmt::Error> {
    schema_name_from_exprs_inner(exprs, ", ")
}

fn schema_name_from_exprs_inner(exprs: &[Expr], sep: &str) -> Result<String, fmt::Error> {
    let mut s = String::new();
    for (i, e) in exprs.iter().enumerate() {
        if i > 0 {
            write!(&mut s, "{sep}")?;
        }
        write!(&mut s, "{}", SchemaDisplay(e))?;
    }

    Ok(s)
}

pub fn schema_name_from_sorts(sorts: &[Sort]) -> Result<String, fmt::Error> {
    let mut s = String::new();
    for (i, e) in sorts.iter().enumerate() {
        if i > 0 {
            write!(&mut s, ", ")?;
        }
        let ordering = if e.asc { "ASC" } else { "DESC" };
        let nulls_ordering = if e.nulls_first {
            "NULLS FIRST"
        } else {
            "NULLS LAST"
        };
        write!(&mut s, "{} {} {}", e.expr, ordering, nulls_ordering)?;
    }

    Ok(s)
}

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(Alias { expr, name, .. }) => write!(f, "{expr} AS {name}"),
            Expr::Column(c) => write!(f, "{c}"),
            Expr::OuterReferenceColumn(_, c) => write!(f, "outer_ref({c})"),
            Expr::ScalarVariable(_, var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Case(case) => {
                write!(f, "CASE ")?;
                if let Some(e) = &case.expr {
                    write!(f, "{e} ")?;
                }
                for (w, t) in &case.when_then_expr {
                    write!(f, "WHEN {w} THEN {t} ")?;
                }
                if let Some(e) = &case.else_expr {
                    write!(f, "ELSE {e} ")?;
                }
                write!(f, "END")
            }
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "CAST({expr} AS {data_type:?})")
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                write!(f, "TRY_CAST({expr} AS {data_type:?})")
            }
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::Negative(expr) => write!(f, "(- {expr})"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsUnknown(expr) => write!(f, "{expr} IS UNKNOWN"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::IsNotUnknown(expr) => write!(f, "{expr} IS NOT UNKNOWN"),
            Expr::Exists(Exists {
                subquery,
                negated: true,
            }) => write!(f, "NOT EXISTS ({subquery:?})"),
            Expr::Exists(Exists {
                subquery,
                negated: false,
            }) => write!(f, "EXISTS ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: true,
            }) => write!(f, "{expr} NOT IN ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: false,
            }) => write!(f, "{expr} IN ({subquery:?})"),
            Expr::ScalarSubquery(subquery) => write!(f, "({subquery:?})"),
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::ScalarFunction(fun) => {
                fmt_function(f, fun.name(), false, &fun.args, true)
            }
            // TODO: use udf's display_name, need to fix the seperator issue, <https://github.com/apache/datafusion/issues/10364>
            // Expr::ScalarFunction(ScalarFunction { func, args }) => {
            //     write!(f, "{}", func.display_name(args).unwrap())
            // }
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            }) => {
                fmt_function(f, &fun.to_string(), false, args, true)?;

                if let Some(nt) = null_treatment {
                    write!(f, "{}", nt)?;
                }

                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY [{}]", expr_vec_fmt!(partition_by))?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(order_by))?;
                }
                write!(
                    f,
                    " {} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, window_frame.end_bound
                )?;
                Ok(())
            }
            Expr::AggregateFunction(AggregateFunction {
                func,
                distinct,
                ref args,
                filter,
                order_by,
                null_treatment,
                ..
            }) => {
                fmt_function(f, func.name(), *distinct, args, true)?;
                if let Some(nt) = null_treatment {
                    write!(f, " {}", nt)?;
                }
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
                }
                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                write!(f, "{expr}")?;
                let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " {op_name} {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " {op_name} {pattern}")
                }
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " SIMILAR TO {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " SIMILAR TO {pattern}")
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT IN ([{}])", expr_vec_fmt!(list))
                } else {
                    write!(f, "{expr} IN ([{}])", expr_vec_fmt!(list))
                }
            }
            Expr::Wildcard { qualifier, options } => match qualifier {
                Some(qualifier) => write!(f, "{qualifier}.*{options}"),
                None => write!(f, "*{options}"),
            },
            Expr::GroupingSet(grouping_sets) => match grouping_sets {
                GroupingSet::Rollup(exprs) => {
                    // ROLLUP (c0, c1, c2)
                    write!(f, "ROLLUP ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::Cube(exprs) => {
                    // CUBE (c0, c1, c2)
                    write!(f, "CUBE ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    // GROUPING SETS ((c0), (c1, c2), (c3, c4))
                    write!(
                        f,
                        "GROUPING SETS ({})",
                        lists_of_exprs
                            .iter()
                            .map(|exprs| format!("({})", expr_vec_fmt!(exprs)))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
            },
            Expr::Placeholder(Placeholder { id, .. }) => write!(f, "{id}"),
            Expr::Unnest(Unnest { expr }) => {
                write!(f, "UNNEST({expr})")
            }
        }
    }
}

fn fmt_function(
    f: &mut fmt::Formatter,
    fun: &str,
    distinct: bool,
    args: &[Expr],
    display: bool,
) -> fmt::Result {
    let args: Vec<String> = match display {
        true => args.iter().map(|arg| format!("{arg}")).collect(),
        false => args.iter().map(|arg| format!("{arg:?}")).collect(),
    };

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

/// The name of the column (field) that this `Expr` will produce in the physical plan.
/// The difference from [Expr::schema_name] is that top-level columns are unqualified.
pub fn physical_name(expr: &Expr) -> Result<String> {
    if let Expr::Column(col) = expr {
        Ok(col.name.clone())
    } else {
        Ok(expr.schema_name().to_string())
    }
}

#[cfg(test)]
mod test {
    use crate::expr_fn::col;
    use crate::{
        case, lit, qualified_wildcard, wildcard, wildcard_with_options, ColumnarValue,
        ScalarUDF, ScalarUDFImpl, Volatility,
    };
    use sqlparser::ast;
    use sqlparser::ast::{Ident, IdentWithAlias};
    use std::any::Any;

    #[test]
    #[allow(deprecated)]
    fn format_case_when() -> Result<()> {
        let expr = case(col("a"))
            .when(lit(1), lit(true))
            .when(lit(0), lit(false))
            .otherwise(lit(ScalarValue::Null))?;
        let expected = "CASE a WHEN Int32(1) THEN Boolean(true) WHEN Int32(0) THEN Boolean(false) ELSE NULL END";
        assert_eq!(expected, expr.canonical_name());
        assert_eq!(expected, format!("{expr}"));
        Ok(())
    }

    #[test]
    #[allow(deprecated)]
    fn format_cast() -> Result<()> {
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Float32(Some(1.23)))),
            data_type: DataType::Utf8,
        });
        let expected_canonical = "CAST(Float32(1.23) AS Utf8)";
        assert_eq!(expected_canonical, expr.canonical_name());
        assert_eq!(expected_canonical, format!("{expr}"));
        // Note that CAST intentionally has a name that is different from its `Display`
        // representation. CAST does not change the name of expressions.
        assert_eq!("Float32(1.23)", expr.schema_name().to_string());
        Ok(())
    }

    #[test]
    fn test_partial_ord() {
        // Test validates that partial ord is defined for Expr, not
        // intended to exhaustively test all possibilities
        let exp1 = col("a") + lit(1);
        let exp2 = col("a") + lit(2);
        let exp3 = !(col("a") + lit(2));

        assert!(exp1 < exp2);
        assert!(exp3 > exp2);
        assert!(exp1 < exp3)
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        // single column
        {
            let expr = &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64));
            let columns = expr.column_refs();
            assert_eq!(1, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
        }

        // multiple columns
        {
            let expr = col("a") + col("b") + lit(1);
            let columns = expr.column_refs();
            assert_eq!(2, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
            assert!(columns.contains(&Column::from_name("b")));
        }

        Ok(())
    }

    #[test]
    fn test_logical_ops() {
        assert_eq!(
            format!("{}", lit(1u32).eq(lit(2u32))),
            "UInt32(1) = UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).not_eq(lit(2u32))),
            "UInt32(1) != UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt(lit(2u32))),
            "UInt32(1) > UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt_eq(lit(2u32))),
            "UInt32(1) >= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt(lit(2u32))),
            "UInt32(1) < UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt_eq(lit(2u32))),
            "UInt32(1) <= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).and(lit(2u32))),
            "UInt32(1) AND UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).or(lit(2u32))),
            "UInt32(1) OR UInt32(2)"
        );
    }

    #[test]
    fn test_is_volatile_scalar_func() {
        // UDF
        #[derive(Debug)]
        struct TestScalarUDF {
            signature: Signature,
        }
        impl ScalarUDFImpl for TestScalarUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "TestScalarUDF"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Utf8)
            }

            fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::from("a")))
            }
        }
        let udf = Arc::new(ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        }));
        assert_ne!(udf.signature().volatility, Volatility::Volatile);

        let udf = Arc::new(ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));
        assert_eq!(udf.signature().volatility, Volatility::Volatile);
    }

    use super::*;

    #[test]
    fn test_first_value_return_type() -> Result<()> {
        let fun = find_df_window_func("first_value").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[true], "")?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::UInt64], &[true], "")?;
        assert_eq!(DataType::UInt64, observed);

        Ok(())
    }

    #[test]
    fn test_last_value_return_type() -> Result<()> {
        let fun = find_df_window_func("last_value").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[true], "")?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[true], "")?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lead_return_type() -> Result<()> {
        let fun = find_df_window_func("lead").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[true], "")?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[true], "")?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_lag_return_type() -> Result<()> {
        let fun = find_df_window_func("lag").unwrap();
        let observed = fun.return_type(&[DataType::Utf8], &[true], "")?;
        assert_eq!(DataType::Utf8, observed);

        let observed = fun.return_type(&[DataType::Float64], &[true], "")?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_nth_value_return_type() -> Result<()> {
        let fun = find_df_window_func("nth_value").unwrap();
        let observed =
            fun.return_type(&[DataType::Utf8, DataType::UInt64], &[true, true], "")?;
        assert_eq!(DataType::Utf8, observed);

        let observed =
            fun.return_type(&[DataType::Float64, DataType::UInt64], &[true, true], "")?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_cume_dist_return_type() -> Result<()> {
        let fun = find_df_window_func("cume_dist").unwrap();
        let observed = fun.return_type(&[], &[], "")?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_ntile_return_type() -> Result<()> {
        let fun = find_df_window_func("ntile").unwrap();
        let observed = fun.return_type(&[DataType::Int16], &[true], "")?;
        assert_eq!(DataType::UInt64, observed);

        Ok(())
    }

    #[test]
    fn test_window_function_case_insensitive() -> Result<()> {
        let names = vec![
            "cume_dist",
            "ntile",
            "lag",
            "lead",
            "first_value",
            "last_value",
            "nth_value",
        ];
        for name in names {
            let fun = find_df_window_func(name).unwrap();
            let fun2 = find_df_window_func(name.to_uppercase().as_str()).unwrap();
            assert_eq!(fun, fun2);
            if fun.to_string() == "first_value" || fun.to_string() == "last_value" {
                assert_eq!(fun.to_string(), name);
            } else {
                assert_eq!(fun.to_string(), name.to_uppercase());
            }
        }
        Ok(())
    }

    #[test]
    fn test_find_df_window_function() {
        assert_eq!(
            find_df_window_func("cume_dist"),
            Some(WindowFunctionDefinition::BuiltInWindowFunction(
                built_in_window_function::BuiltInWindowFunction::CumeDist
            ))
        );
        assert_eq!(
            find_df_window_func("first_value"),
            Some(WindowFunctionDefinition::BuiltInWindowFunction(
                built_in_window_function::BuiltInWindowFunction::FirstValue
            ))
        );
        assert_eq!(
            find_df_window_func("LAST_value"),
            Some(WindowFunctionDefinition::BuiltInWindowFunction(
                built_in_window_function::BuiltInWindowFunction::LastValue
            ))
        );
        assert_eq!(
            find_df_window_func("LAG"),
            Some(WindowFunctionDefinition::BuiltInWindowFunction(
                built_in_window_function::BuiltInWindowFunction::Lag
            ))
        );
        assert_eq!(
            find_df_window_func("LEAD"),
            Some(WindowFunctionDefinition::BuiltInWindowFunction(
                built_in_window_function::BuiltInWindowFunction::Lead
            ))
        );
        assert_eq!(find_df_window_func("not_exist"), None)
    }

    #[test]
    fn test_display_wildcard() {
        assert_eq!(format!("{}", wildcard()), "*");
        assert_eq!(format!("{}", qualified_wildcard("t1")), "t1.*");
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    Some(IlikeSelectItem {
                        pattern: "c1".to_string()
                    }),
                    None,
                    None,
                    None,
                    None
                ))
            ),
            "* ILIKE 'c1'"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    Some(ExcludeSelectItem::Multiple(vec![
                        Ident::from("c1"),
                        Ident::from("c2")
                    ])),
                    None,
                    None,
                    None
                ))
            ),
            "* EXCLUDE (c1, c2)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    Some(ExceptSelectItem {
                        first_element: Ident::from("c1"),
                        additional_elements: vec![Ident::from("c2")]
                    }),
                    None,
                    None
                ))
            ),
            "* EXCEPT (c1, c2)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    None,
                    Some(PlannedReplaceSelectItem {
                        items: vec![ReplaceSelectElement {
                            expr: ast::Expr::Identifier(Ident::from("c1")),
                            column_name: Ident::from("a1"),
                            as_keyword: false
                        }],
                        planned_expressions: vec![]
                    }),
                    None
                ))
            ),
            "* REPLACE (c1 a1)"
        );
        assert_eq!(
            format!(
                "{}",
                wildcard_with_options(wildcard_options(
                    None,
                    None,
                    None,
                    None,
                    Some(RenameSelectItem::Multiple(vec![IdentWithAlias {
                        ident: Ident::from("c1"),
                        alias: Ident::from("a1")
                    }]))
                ))
            ),
            "* RENAME (c1 AS a1)"
        )
    }

    fn wildcard_options(
        opt_ilike: Option<IlikeSelectItem>,
        opt_exclude: Option<ExcludeSelectItem>,
        opt_except: Option<ExceptSelectItem>,
        opt_replace: Option<PlannedReplaceSelectItem>,
        opt_rename: Option<RenameSelectItem>,
    ) -> WildcardOptions {
        WildcardOptions {
            ilike: opt_ilike,
            exclude: opt_exclude,
            except: opt_except,
            replace: opt_replace,
            rename: opt_rename,
        }
    }
}