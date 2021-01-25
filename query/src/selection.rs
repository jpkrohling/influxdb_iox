//! This represents selecting a set of columns
pub enum Selection<'a> {
    /// Return all columns (e.g. SELECT *)
    AllColumns,

    /// Return only the named columns
    SpecifiedColumns(&'a [&'a str]),
}

impl From<&[&str]> for Selection<'_> {
    fn from(names: &[&str]) -> Self {
        Self::SpecifiedColumns(names)
    }
}
