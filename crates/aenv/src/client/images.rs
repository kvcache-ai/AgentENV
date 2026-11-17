use super::{handle_status, Client};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize)]
struct ImagePullRequest<'a> {
    image: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct PulledImage {
    /// The published copy, such as `aenv/docker.io/library/python:3.12`.
    pub image: String,
    #[serde(rename = "imageDigest")]
    pub image_digest: String,
}

impl Client {
    /// Pulls `image` on the server and publishes it to the shared repository.
    pub fn pull_image(&self, image: &str, timeout: Duration) -> Result<PulledImage> {
        let resp = handle_status(
            self.post("/images/pulls")
                .timeout(timeout)
                .send_json(ImagePullRequest { image }),
        )?;
        Ok(resp.into_json()?)
    }
}
