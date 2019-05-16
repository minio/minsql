use futures::Future;
use futures::future::FutureResult;
use futures::future::result;
use futures::Poll;
use rusoto_core::HttpClient;
use rusoto_core::Region;
use rusoto_credential::AwsCredentials;
use rusoto_credential::CredentialsError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_s3::{ListObjectsRequest, S3, S3Client};

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

// <p>Function used to verify if a datastore is valid in terms of reachability</p>
pub fn can_reach_datastore(datastore: &DataStore) -> bool {
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

//pub fn write_to_datastore(datastore: &DataStore, payload: &String) {
//    println!("Writing to datastore");
//}