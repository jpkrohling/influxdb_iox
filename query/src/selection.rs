#[derive(Debug, Clone)]
/// This represents the request to select set of columns from a table
pub enum Selection<'a> {
    /// Return all columns (e.g. SELECT *)
    /// The columns are sorted lexographically by name
    All,

    /// Return only the named columns
    Some(&'a [&'a str]),
}
