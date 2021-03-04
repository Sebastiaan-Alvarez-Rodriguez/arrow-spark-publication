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

//! Execution plan for reading Parquet files

use std::any::Any;
use std::fmt;
use std::fs::File;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{RecordBatchStream, SendableRecordBatchStream};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{common, Partitioning};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::file::reader::SerializedFileReader;

use crossbeam::channel::{bounded, Receiver, RecvError, Sender};
use fmt::Debug;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use tokio::task;

use crate::datasource::datasource::Statistics;
use async_trait::async_trait;
use futures::stream::Stream;

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Parquet partitions to read
    partitions: Vec<ParquetPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
    /// Statistics for the data set (sum of statistics for all partitions)
    statistics: Statistics,
}

/// Represents one partition of a Parquet data set and this currently means one Parquet file.
///
/// In the future it would be good to support subsets of files based on ranges of row groups
/// so that we can better parallelize reads of large files across available cores (see
/// [ARROW-10995](https://issues.apache.org/jira/browse/ARROW-10995)).
///
/// We may also want to support reading Parquet files that are partitioned based on a key and
/// in this case we would want this partition struct to represent multiple files for a given
/// partition key (see [ARROW-11019](https://issues.apache.org/jira/browse/ARROW-11019)).
#[derive(Debug, Clone)]
pub struct ParquetPartition {
    /// The Parquet filename for this partition
    filenames: Vec<String>,
    /// Statistics for this partition
    statistics: Statistics,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan based on the specified Parquet filename or
    /// directory containing Parquet files
    pub fn try_from_path(
        path: &str,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        max_concurrency: usize,
    ) -> Result<Self> {
        // build a list of filenames from the specified path, which could be a single file or
        // a directory containing one or more parquet files
        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, ".parquet")?;
        if filenames.is_empty() {
            Err(DataFusionError::Plan(format!(
                "No Parquet files found at path {}",
                path
            )))
        } else {
            let filenames = filenames
                .iter()
                .map(|filename| filename.as_str())
                .collect::<Vec<&str>>();
            Self::try_from_files(&filenames, projection, batch_size, max_concurrency)
        }
    }

    /// Create a new Parquet reader execution plan based on the specified list of Parquet
    /// files
    pub fn try_from_files(
        filenames: &[&str],
        projection: Option<Vec<usize>>,
        batch_size: usize,
        max_concurrency: usize,
    ) -> Result<Self> {
        // build a list of Parquet partitions with statistics and gather all unique schemas
        // used in this data set
        let mut schemas: Vec<Schema> = vec![];
        let mut partitions = Vec::with_capacity(max_concurrency);
        let filenames: Vec<String> = filenames.iter().map(|s| s.to_string()).collect();
        let chunks = split_files(&filenames, max_concurrency);
        let mut num_rows = 0;
        let mut total_byte_size = 0;
        for chunk in chunks {
            let filenames: Vec<String> = chunk.iter().map(|x| x.to_string()).collect();
            for filename in &filenames {
                let file = File::open(filename)?;
                let file_reader = Arc::new(SerializedFileReader::new(file)?);
                let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                let meta_data = arrow_reader.get_metadata();
                // collect all the unique schemas in this data set
                let schema = arrow_reader.get_schema()?;
                if schemas.is_empty() || schema != schemas[0] {
                    schemas.push(schema);
                }
                for i in 0..meta_data.num_row_groups() {
                    let row_group_meta = meta_data.row_group(i);
                    num_rows += row_group_meta.num_rows();
                    total_byte_size += row_group_meta.total_byte_size();
                }
            }
            let statistics = Statistics {
                num_rows: Some(num_rows as usize),
                total_byte_size: Some(total_byte_size as usize),
                column_statistics: None,
            };
            partitions.push(ParquetPartition {
                filenames,
                statistics,
            });
        }

        // we currently get the schema information from the first file rather than do
        // schema merging and this is a limitation.
        // See https://issues.apache.org/jira/browse/ARROW-11017
        if schemas.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "The Parquet files have {} different schemas and DataFusion does \
                not yet support schema merging",
                schemas.len()
            )));
        }
        let schema = schemas[0].clone();

        Ok(Self::new(partitions, schema, projection, batch_size))
    }

    /// Create a new Parquet reader execution plan with provided partitions and schema
    pub fn new(
        partitions: Vec<ParquetPartition>,
        schema: Schema,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        // sum the statistics
        let mut num_rows: Option<usize> = None;
        let mut total_byte_size: Option<usize> = None;
        for part in &partitions {
            if let Some(n) = part.statistics.num_rows {
                num_rows = Some(num_rows.unwrap_or(0) + n)
            }
            if let Some(n) = part.statistics.total_byte_size {
                total_byte_size = Some(total_byte_size.unwrap_or(0) + n)
            }
        }
        let statistics = Statistics {
            num_rows,
            total_byte_size,
            column_statistics: None,
        };
        Self {
            partitions,
            schema: Arc::new(projected_schema),
            projection,
            batch_size,
            statistics,
        }
    }

    /// Parquet partitions to read
    pub fn partitions(&self) -> &[ParquetPartition] {
        &self.partitions
    }

    /// Projection for which columns to load
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Statistics for the data set (sum of statistics for all partitions)
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

impl ParquetPartition {
    /// The Parquet filename for this partition
    pub fn filenames(&self) -> &[String] {
        &self.filenames
    }

    /// Statistics for this partition
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (
            Sender<Option<ArrowResult<RecordBatch>>>,
            Receiver<Option<ArrowResult<RecordBatch>>>,
        ) = bounded(2);

        let filenames = self.partitions[partition].filenames.clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;

        task::spawn_blocking(move || {
            if let Err(e) = read_files(&filenames, &projection, batch_size, response_tx) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream {
            schema: self.schema.clone(),
            response_rx,
        }))
    }
}

fn send_result(
    response_tx: &Sender<Option<ArrowResult<RecordBatch>>>,
    result: Option<ArrowResult<RecordBatch>>,
) -> Result<()> {
    response_tx
        .send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

fn read_files(
    filenames: &[String],
    projection: &[usize],
    batch_size: usize,
    response_tx: Sender<Option<ArrowResult<RecordBatch>>>,
) -> Result<()> {
    for filename in filenames {
        let file = File::open(&filename)?;
        let file_reader = Arc::new(SerializedFileReader::new(file)?);
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.to_owned(), batch_size)?;
        loop {
            match batch_reader.next() {
                Some(Ok(batch)) => {
                    //println!("ParquetExec got new batch from {}", filename);
                    send_result(&response_tx, Some(Ok(batch)))?
                }
                None => {
                    break;
                }
                Some(Err(e)) => {
                    let err_msg = format!(
                        "Error reading batch from {}: {}",
                        filename,
                        e.to_string()
                    );
                    // send error to operator
                    send_result(
                        &response_tx,
                        Some(Err(ArrowError::ParquetError(err_msg.clone()))),
                    )?;
                    // terminate thread with error
                    return Err(DataFusionError::Execution(err_msg));
                }
            }
        }
    }

    // finished reading files
    send_result(&response_tx, None)?;

    Ok(())
}

fn split_files(filenames: &[String], n: usize) -> Vec<&[String]> {
    let mut chunk_size = filenames.len() / n;
    if filenames.len() % n > 0 {
        chunk_size += 1;
    }
    filenames.chunks(chunk_size).collect()
}

struct ParquetStream {
    schema: SchemaRef,
    response_rx: Receiver<Option<ArrowResult<RecordBatch>>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.response_rx.recv() {
            Ok(batch) => Poll::Ready(batch),
            // RecvError means receiver has exited and closed the channel
            Err(RecvError) => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn test_split_files() {
        let filenames = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        let chunks = split_files(&filenames, 1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = split_files(&filenames, 2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = split_files(&filenames, 5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(&filenames, 123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let testdata = arrow::util::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec =
            ParquetExec::try_from_path(&filename, Some(vec![0, 1, 2]), 1024, 4)?;
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
