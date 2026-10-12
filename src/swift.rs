// Copyright 2026 Rackspace US, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

use anyhow::{bail, Context, Result};
use reqwest::{Client, StatusCode};
use std::time::Duration;

#[derive(Clone)]
pub struct SwiftClient {
    client: Client,
    endpoint: String,
    token: String,
}

impl SwiftClient {
    pub fn new(endpoint: String, token: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .pool_max_idle_per_host(64)
            .build()
            .expect("Failed to initialize reqwest client");

        Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token,
        }
    }

    fn object_url(&self, container: &str, object_name: &str) -> String {
        format!("{}/{}/{}", self.endpoint, container, object_name)
    }

    fn container_url(&self, container: &str) -> String {
        format!("{}/{}", self.endpoint, container)
    }

    pub async fn ensure_container(&self, container: &str) -> Result<()> {
        let url = self.container_url(container);
        let resp = self
            .client
            .put(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("Failed to ensure container")?;

        if resp.status().is_success() || resp.status() == StatusCode::ACCEPTED {
            Ok(())
        } else {
            bail!("Failed to create container {}: {}", container, resp.status());
        }
    }

    pub async fn head_object(&self, container: &str, object_name: &str) -> Result<bool> {
        let url = self.object_url(container, object_name);
        let resp = self
            .client
            .head(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("HEAD object request failed")?;

        if resp.status().is_success() {
            Ok(true)
        } else if resp.status() == StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            bail!("HEAD object {} error: {}", object_name, resp.status());
        }
    }

    pub async fn put_object(&self, container: &str, object_name: &str, data: Vec<u8>) -> Result<()> {
        let url = self.object_url(container, object_name);
        let resp = self
            .client
            .put(&url)
            .header("X-Auth-Token", &self.token)
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await
            .context("PUT object request failed")?;

        if resp.status().is_success() || resp.status() == StatusCode::CREATED {
            Ok(())
        } else {
            bail!("PUT object {} failed: {}", object_name, resp.status());
        }
    }

    pub async fn get_object(&self, container: &str, object_name: &str) -> Result<Vec<u8>> {
        let url = self.object_url(container, object_name);
        let resp = self
            .client
            .get(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("GET object request failed")?;

        if resp.status().is_success() {
            let bytes = resp.bytes().await.context("Failed to read body bytes")?;
            Ok(bytes.to_vec())
        } else {
            bail!("GET object {} failed: {}", object_name, resp.status());
        }
    }

    #[allow(dead_code)]
    pub async fn delete_object(&self, container: &str, object_name: &str) -> Result<()> {
        let url = self.object_url(container, object_name);
        let resp = self
            .client
            .delete(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("DELETE object request failed")?;

        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            bail!("DELETE object {} failed: {}", object_name, resp.status());
        }
    }
}
