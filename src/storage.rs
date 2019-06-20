// This file is part of MinSQL
// Copyright (c) 2019 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, RwLock};
use std::time::Instant;

use chrono::{Datelike, Timelike, Utc};
use futures::future::result;
use futures::future::FutureResult;
use futures::Future;
use futures::Poll;
use log::error;
use rand::distributions::{IndependentSample, Range};
use rusoto_core::HttpClient;
use rusoto_core::Region;
use rusoto_core::RusotoError;
use rusoto_credential::AwsCredentials;
use rusoto_credential::CredentialsError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_s3::{
    GetObjectError, GetObjectRequest, ListObjectsRequest, PutObjectRequest, S3Client, S3,
};
use tokio_codec::{FramedRead, LinesCodec};
use tokio_io::AsyncRead;
use uuid::Uuid;

use crate::config::{Config, DataStore};

#[derive(Debug)]
pub enum StorageError<E> {
    // Wraps around an error that happened for a specific operation
    Operation(E),
    // A validation error happened
    Validation(String),
    Unhandled,
}

/// Maps a `rusoto_s3::GetObjectError` to a `StorageError<GetObjectError>`
impl From<RusotoError<rusoto_s3::GetObjectError>> for StorageError<GetObjectError> {
    fn from(err: RusotoError<rusoto_s3::GetObjectError>) -> Self {
        match err {
            RusotoError::Service(se) => match se {
                rusoto_s3::GetObjectError::NoSuchKey(s) => {
                    StorageError::Operation(GetObjectError::NoSuchKey(s))
                }
            },
            _ => StorageError::Unhandled,
        }
    }
}

// Our Credentials holder so we can use per-datasource credentials with rusoto
#[derive(Debug)]
pub struct CustomCredentialsProvider {
    credentials: AwsCredentials,
}

impl CustomCredentialsProvider {
    pub fn with_credentials(credentials: AwsCredentials) -> Self {
        CustomCredentialsProvider {
            credentials: credentials,
        }
    }
}

pub struct CustomCredentialsProviderFuture {
    inner: FutureResult<AwsCredentials, CredentialsError>,
}

impl Future for CustomCredentialsProviderFuture {
    type Item = AwsCredentials;
    type Error = CredentialsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl ProvideAwsCredentials for CustomCredentialsProvider {
    type Future = CustomCredentialsProviderFuture;

    fn credentials(&self) -> Self::Future {
        CustomCredentialsProviderFuture {
            inner: result(Ok(self.credentials.clone())),
        }
    }
}

pub fn client_for_datastore(datastore: &DataStore) -> S3Client {
    // Create a credentials holder, for our provider to provide into the s3 client
    let credentials = AwsCredentials::new(
        &datastore.access_key[..],
        &datastore.secret_key[..],
        None,
        None,
    );
    let provider = CustomCredentialsProvider::with_credentials(credentials);
    let dispatcher = HttpClient::new().expect("failed to create request dispatcher");
    // A custom region is the way to point to a minio instance
    let region = Region::Custom {
        name: datastore.name.clone().unwrap(),
        endpoint: datastore.endpoint.clone(),
    };
    // Build the client
    S3Client::new_with(dispatcher, provider, region)
}

#[derive(Debug)]
pub enum ReachableDatastoreError {
    NoSuchBucket(String),
}

/// <p>Function used to verify if a datastore is valid in terms of reachability</p>
pub fn can_reach_datastore(
    datastore: &DataStore,
) -> Result<bool, StorageError<ReachableDatastoreError>> {
    // Get the Object Storage client
    let s3_client = client_for_datastore(&datastore);
    // perform list call to verify we have access
    s3_client
        .list_objects(ListObjectsRequest {
            bucket: datastore.bucket.clone(),
            delimiter: None,
            encoding_type: None,
            marker: None,
            max_keys: Some(i64::from(1)),
            prefix: None,
            request_payer: None,
        })
        .sync()
        .map_err(|e| {
            error!("Cannot access bucket: {}", e);
            match e {
                RusotoError::Service(se) => match se {
                    rusoto_s3::ListObjectsError::NoSuchBucket(s) => {
                        StorageError::Operation(ReachableDatastoreError::NoSuchBucket(s))
                    }
                },
                RusotoError::Validation(s) => StorageError::Validation(s),
                _ => StorageError::Unhandled,
            }
        })
        .map(|_| Ok(true))
        .unwrap_or(Ok(false))
}

fn str_to_streaming_body(s: String) -> rusoto_s3::StreamingBody {
    s.into_bytes().into()
}

#[derive(Debug)]
pub enum PutObjectError {
    Write(String),
}

pub fn write_to_datastore(
    cfg: Arc<RwLock<Config>>,
    log_name: &str,
    payload: &String,
) -> Result<bool, StorageError<PutObjectError>> {
    let start = Instant::now();
    let read_cfg = cfg.read().unwrap();
    // Select a datastore at random to write to
    let datastore = rand_datastore(&read_cfg, &log_name).unwrap();
    // Get the Object Storage client
    let s3_client = client_for_datastore(&datastore);
    let now = Utc::now();
    let my_uuid = Uuid::new_v4();
    let target_file = format!(
        "{log}/{year}/{month}/{day}/{hour}/{ts}.log",
        log = log_name,
        year = now.date().year(),
        month = now.date().month(),
        day = now.date().day(),
        hour = now.hour(),
        ts = my_uuid
    );
    let destination = format!("minsql/{}", target_file);
    // turn the payload into a streaming body
    let strbody = str_to_streaming_body(payload.clone());
    // save the payload
    // TODO: Make this function return a stream so we can switch to an async response and not block
    let save_res = s3_client
        .put_object(PutObjectRequest {
            bucket: datastore.bucket.clone(),
            key: destination,
            body: Some(strbody),
            ..Default::default()
        })
        .sync();
    save_res
        .map_err(|e| {
            StorageError::Operation(PutObjectError::Write(format!(
                "Could not write to datastore: {}",
                e
            )))
        })
        .map(|_| {
            //TODO: Remove this metric
            let duration = start.elapsed();
            println!("Writing to minio: {:?}", duration);
            true
        })
}

#[derive(Debug)]
pub enum ListObjectsError {
    List(String),
}

// List all the files for a bucket
pub fn list_msl_bucket_files(
    logname: &str,
    datastore: &DataStore,
) -> Result<Vec<String>, StorageError<ListObjectsError>> {
    let s3_client = client_for_datastore(datastore);
    // TODO: Make this function return a stream so we can switch to an async response and not block
    let objects_res = s3_client
        .list_objects(ListObjectsRequest {
            bucket: datastore.bucket.clone(),
            prefix: Some(format!("minsql/{}", logname)),
            ..Default::default()
        })
        .sync();
    objects_res
        .map(|objects| {
            objects
                .contents
                .unwrap()
                .iter()
                .map(|f| f.clone().key.unwrap())
                .filter(|f| f.contains(".log"))
                .collect()
        })
        .map_err(|e| {
            StorageError::Operation(ListObjectsError::List(format!(
                "Could not list in datastore: {}",
                e
            )))
        })
}

#[derive(Debug)]
pub enum GetObjectError {
    NoSuchKey(String),
}

// Read file in object store and return its contents as a stream of
// lines.
pub fn read_file_line_by_line(
    key: &String,
    datastore: &DataStore,
) -> Result<FramedRead<impl AsyncRead, LinesCodec>, StorageError<GetObjectError>> {
    let s3_client = client_for_datastore(datastore);
    let get_object_res = s3_client
        .get_object(GetObjectRequest {
            bucket: datastore.bucket.clone(),
            key: key.clone(),
            ..Default::default()
        })
        .sync();

    get_object_res
        .map(|f| {
            FramedRead::new(
                f.body.unwrap().into_async_read(),
                // max line length of 1MiB
                LinesCodec::new_with_max_length(1024 * 1024),
            )
        })
        .map_err(|e| e.into())
}

/// Selects a datastore at random. Will return `None` if the log_name
/// doesn't match a valid `Log` name in the `Config`.
fn rand_datastore<'a>(cfg: &'a Config, log_name: &str) -> Option<&'a DataStore> {
    cfg.log
        .get(log_name)
        .and_then(|log| {
            let n = log.datastores.len();
            let i = Range::new(0, n).ind_sample(&mut rand::thread_rng());
            log.datastores.iter().skip(i).next()
        })
        .and_then(|name| cfg.datastore.get(&name[..]))
}

#[cfg(test)]
mod storage_tests {
    use std::collections::HashMap;

    use crate::config::Log;

    use super::*;

    // Generates a Config object with only one auth item for one log
    fn get_ds_log_config_for(log_name: String, datastore_list: &Vec<String>) -> Config {
        let mut datastore_map = HashMap::new();
        for datastore_name in datastore_list {
            datastore_map.insert(
                datastore_name.clone(),
                DataStore {
                    name: Some(datastore_name.clone()),
                    endpoint: "".to_string(),
                    access_key: "".to_string(),
                    secret_key: "".to_string(),
                    bucket: "".to_string(),
                    prefix: "".to_string(),
                },
            );
        }

        let mut log_map = HashMap::new();
        log_map.insert(
            log_name.clone(),
            Log {
                name: Some(log_name.clone()),
                datastores: datastore_list.clone(),
                commit_window: "5s".to_string(),
            },
        );

        let cfg = Config {
            version: "1".to_string(),
            server: None,
            datastore: datastore_map,
            log: log_map,
            auth: HashMap::new(),
        };
        cfg
    }

    #[test]
    fn random_datastore_selected() {
        let ds_list = vec!["ds1".to_string(), "ds2".to_string()];
        let cfg = get_ds_log_config_for("mylog".to_string(), &ds_list);
        let cfg = Box::new(cfg);
        let cfg: &'static _ = Box::leak(cfg);

        let rand_ds = rand_datastore(&cfg, "mylog");
        let ds_name = match rand_ds {
            None => panic!("No datastore was matched"),
            Some(ds) => ds.name.clone().unwrap(),
        };
        let ds_in_list = ds_list.contains(&ds_name);
        assert_eq!(ds_in_list, true)
    }

    #[test]
    fn fail_random_datastore_selected() {
        let ds_list = vec!["ds1".to_string(), "ds2".to_string()];
        let cfg = get_ds_log_config_for("mylog".to_string(), &ds_list);
        let cfg = Box::new(cfg);
        let cfg: &'static _ = Box::leak(cfg);

        let rand_ds = rand_datastore(&cfg, "mylog2");
        assert_eq!(
            rand_ds, None,
            "Select random datastore from incorrect log should have failed."
        )
    }
}
