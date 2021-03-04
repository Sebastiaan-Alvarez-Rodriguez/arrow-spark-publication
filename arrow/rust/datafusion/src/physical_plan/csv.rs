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

//! Execution plan for reading CSV files

use std::any::Any;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{common, Partitioning};
use arrow::csv;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use futures::Stream;

use super::{RecordBatchStream, SendableRecordBatchStream};
use async_trait::async_trait;

/// CSV file read option
#[derive(Copy, Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".csv".
    pub file_extension: &'a str,
}

impl<'a> CsvReadOptions<'a> {
    /// Create a CSV read option with default presets
    pub fn new() -> Self {
        Self {
            has_header: true,
            schema: None,
            schema_infer_max_records: 1000,
            delimiter: b',',
            file_extension: ".csv",
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Specify the file extension for CSV file selection
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Configure delimiter setting with Option, None value will be ignored
    pub fn delimiter_option(mut self, delimiter: Option<u8>) -> Self {
        if let Some(d) = delimiter {
            self.delimiter = d;
        }
        self
    }

    /// Specify schema to use for CSV read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }
}

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    /// Path to directory containing partitioned CSV files with the same schema
    path: String,
    /// The individual files under path
    filenames: Vec<String>,
    /// Schema representing the CSV file
    schema: SchemaRef,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// File extension
    file_extension: String,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
}

impl CsvExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file_extension = String::from(options.file_extension);

        let mut filenames: Vec<String> = vec![];
        common::build_file_list(path, &mut filenames, file_extension.as_str())?;
        if filenames.is_empty() {
            return Err(DataFusionError::Execution("No files found".to_string()));
        }

        let schema = match options.schema {
            Some(s) => s.clone(),
            None => CsvExec::try_infer_schema(&filenames, &options)?,
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            path: path.to_string(),
            filenames,
            schema: Arc::new(schema),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            file_extension,
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
        })
    }

    /// Path to directory containing partitioned CSV files with the same schema
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The individual files under path
    pub fn filenames(&self) -> &[String] {
        &self.filenames
    }

    /// Does the CSV file have a header?
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// An optional column delimiter. Defaults to `b','`
    pub fn delimiter(&self) -> Option<&u8> {
        self.delimiter.as_ref()
    }

    /// File extension
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Optional projection for which columns to load
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Infer schema for given CSV dataset
    pub fn try_infer_schema(
        filenames: &[String],
        options: &CsvReadOptions,
    ) -> Result<Schema> {
        Ok(csv::infer_schema_from_files(
            filenames,
            options.delimiter,
            Some(options.schema_infer_max_records),
            options.has_header,
        )?)
    }
}

#[async_trait]
impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.filenames.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
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
        Ok(Box::pin(CsvStream::try_new(
            &self.filenames[partition],
            self.schema.clone(),
            self.has_header,
            self.delimiter,
            &self.projection,
            self.batch_size,
        )?))
    }
}

/// Iterator over batches
struct CsvStream {
    /// Arrow CSV reader
    reader: csv::Reader<File>,
}

impl CsvStream {
    /// Create an iterator for a CSV file
    pub fn try_new(
        filename: &str,
        schema: SchemaRef,
        has_header: bool,
        delimiter: Option<u8>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let file = File::open(filename)?;
        let reader = csv::Reader::new(
            file,
            schema,
            has_header,
            delimiter,
            batch_size,
            None,
            projection.clone(),
        );

        Ok(Self { reader })
    }
}

impl Stream for CsvStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.reader.next())
    }
}

impl RecordBatchStream for CsvStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::aggr_test_schema;
    use futures::StreamExt;

    #[tokio::test]
    async fn csv_exec_with_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = arrow::util::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            Some(vec![0, 2, 4]),
            1024,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_without_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = arrow::util::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv =
            CsvExec::try_new(&path, CsvReadOptions::new().schema(&schema), None, 1024)?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());
        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(13, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c2", batch_schema.field(1).name());
        assert_eq!("c3", batch_schema.field(2).name());
        Ok(())
    }
}
