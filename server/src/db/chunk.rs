use arrow_deps::{
    arrow::record_batch::RecordBatch, datafusion::logical_plan::LogicalPlan,
    util::str_iter_to_batch,
};
use query::{
    predicate::{Predicate, PredicateBuilder},
    selection::Selection,
    util::make_scan_plan,
    PartitionChunk,
};
use read_buffer::Database as ReadBufferDb;
use snafu::{ResultExt, Snafu};

use std::sync::{Arc, RwLock};

use super::{
    pred::to_read_buffer_predicate,
    selection::{to_mutable_buffer_selection, to_read_buffer_selection},
};

use async_trait::async_trait;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunk {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Internal Predicate Conversion Error: {}", source))]
    InternalPredicateConversion { source: super::pred::Error },

    #[snafu(display("internal error creating plan: {}", source))]
    InternalPlanCreation {
        source: arrow_deps::datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion {
        source: arrow_deps::arrow::error::ArrowError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer {
        chunk: Arc<mutable_buffer::chunk::Chunk>,
    },
    ReadBuffer {
        db: Arc<RwLock<ReadBufferDb>>,
        partition_key: String,
        chunk_id: u32,
    },
    ParquetFile, // TODO add appropriate type here
}

impl DBChunk {
    /// Create a new mutable buffer chunk
    pub fn new_mb(chunk: Arc<mutable_buffer::chunk::Chunk>) -> Arc<Self> {
        Arc::new(Self::MutableBuffer { chunk })
    }

    /// create a new read buffer chunk
    pub fn new_rb(
        db: Arc<RwLock<ReadBufferDb>>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Arc<Self> {
        let partition_key = partition_key.into();
        Arc::new(Self::ReadBuffer {
            db,
            chunk_id,
            partition_key,
        })
    }
}

#[async_trait]
impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk } => chunk.id(),
            Self::ReadBuffer { chunk_id, .. } => *chunk_id,
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(&self) -> Result<Vec<data_types::partition_metadata::Table>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer { .. } => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<(), Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => {
                let mb_selection = to_mutable_buffer_selection(selection);
                chunk
                    .table_to_arrow(dst, table_name, mb_selection)
                    .context(MutableBufferChunk)?;
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                // Translate the predicate and selection to ReadBuffer style
                let predicate = PredicateBuilder::default().build();
                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(InternalPredicateConversion)?;
                let rb_selection = to_read_buffer_selection(selection);

                // run the query
                let db = db.read().unwrap();
                let read_result = db
                    .read_filter(
                        partition_key,
                        table_name,
                        &[chunk_id],
                        rb_predicate,
                        rb_selection,
                    )
                    .context(ReadBufferChunk { chunk_id })?;

                // copy the RecordBatches into dst
                dst.extend(read_result);
            }
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
        Ok(())
    }

    async fn table_names(&self, predicate: &Predicate) -> Result<LogicalPlan, Self::Error> {
        match self {
            Self::MutableBuffer { chunk } => {
                let names: Vec<Option<&str>> = if chunk.is_empty() {
                    Vec::new()
                } else {
                    let chunk_predicate = chunk
                        .compile_predicate(predicate)
                        .context(MutableBufferChunk)?;

                    chunk
                        .table_names(&chunk_predicate)
                        .context(MutableBufferChunk)?
                        .into_iter()
                        .map(Some)
                        .collect()
                };

                let batch = str_iter_to_batch("tables", names).context(ArrowConversion)?;

                make_scan_plan(batch).context(InternalPlanCreation)
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;

                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(InternalPredicateConversion)?;

                let db = db.read().unwrap();
                let batch = db
                    .table_names(partition_key, &[chunk_id], rb_predicate)
                    .context(ReadBufferChunk { chunk_id })?;
                make_scan_plan(batch).context(InternalPlanCreation)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented")
            }
        }
    }
}
