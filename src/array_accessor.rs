/// Trait to abstract over Arrow arrays for the `greatest` function.
///
/// This trait provides access to values within an Arrow array and checks for null values.
/// Implementations of this trait are expected to work with different data types.
pub trait ArrayAccessor {
    /// Type of the value contained in the array.
    type Item;

    /// Returns the value at the specified row, or `None` if the value is null.
    fn value(&self, row: usize) -> Option<Self::Item>;

    /// Returns the number of rows in the array.
    fn len(&self) -> usize;
}