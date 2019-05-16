use chrono::{Datelike, Utc};
use futures::Future;
use futures::future::FutureResult;
use futures::future::result;
use futures::Poll;
use rusoto_core::HttpClient;
use rusoto_core::Region;
use rusoto_credential::AwsCredentials;
use rusoto_credential::CredentialsError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_s3::{ListObjectsRequest, PutObjectRequest, S3, S3Client};

use crate::config::DataStore;

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

fn client_for_datastore(datastore: &DataStore) -> S3Client {
    // Create a credentials holder, for our provider to provide into the s3 client
    let credentials = AwsCredentials::new(
        &datastore.access_key[..],
        &datastore.secret_key[..],
        None,
        None);
    let provider = CustomCredentialsProvider::with_credentials(credentials);
    let dispatcher = HttpClient::new().expect("failed to create request dispatcher");
    // A custom region is the way to point to a minio instance
    let region = Region::Custom {
        name: datastore.name.clone().unwrap(),
        endpoint: datastore.endpoint.clone(),
    };
    // Build the client
    let s3_client = S3Client::new_with(
        dispatcher,
        provider,
        region);
    s3_client
}

// <p>Function used to verify if a datastore is valid in terms of reachability</p>
pub fn can_reach_datastore(datastore: &DataStore) -> bool {
    // Get the Object Storage client
    let s3_client = client_for_datastore(datastore);
    // perform list call to verify we have access
    let can_reach = match s3_client.list_objects(
        ListObjectsRequest {
            bucket: datastore.bucket.clone(),
            delimiter: None,
            encoding_type: None,
            marker: None,
            max_keys: Some(i64::from(1)),
            prefix: None,
            request_payer: None,
        }).sync() {
        Ok(_) => true,
        Err(e) => {
            info!("Cannot access bucket: {}", e);
            false
        }
    };
    can_reach
}

fn str_to_streaming_body(s: String) -> rusoto_s3::StreamingBody {
    s.into_bytes().into()
}

pub fn write_to_datastore(logname: &str, datastore: &DataStore, payload: &String) {
    println!("Writing to datastore {}", payload);
    // Get the Object Storage client
    let s3_client = client_for_datastore(datastore);
    let now = Utc::now();
    let target_file = format!("{log}/{year}/{month}/{day}-{ts}.msl.uncompacted",
                              log = logname,
                              year = now.date().year(),
                              month = now.date().month(),
                              day = now.date().day(),
                              ts = now.timestamp());
    let destination = format!("minsql/{}", target_file);
    println!("Writing to  {}", destination);

    let strbody = str_to_streaming_body(payload.clone());

    s3_client.put_object(PutObjectRequest {
        bucket: datastore.bucket.clone(),
        key: destination,
        body: Some(strbody),
        ..Default::default()
    }).sync().expect("could not upload");

    println!("done");
}