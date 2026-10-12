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
use futures_util::stream::{self, StreamExt};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
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
        let max_retries = 5;
        let mut delay = Duration::from_millis(200);

        for attempt in 1..=max_retries {
            // Fast check: if container already exists, return Ok immediately
            if let Ok(head_resp) = self
                .client
                .head(&url)
                .header("X-Auth-Token", &self.token)
                .send()
                .await
            {
                if head_resp.status().is_success() {
                    return Ok(());
                }
            }

            let resp = self
                .client
                .put(&url)
                .header("X-Auth-Token", &self.token)
                .send()
                .await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() || status == StatusCode::ACCEPTED {
                        return Ok(());
                    }
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        continue;
                    }
                    bail!("Failed to create container {}: {}", container, status);
                }
                Err(_) if attempt < max_retries => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "Failed to ensure container {} after {} attempts",
                        container, max_retries
                    ));
                }
            }
        }
        bail!("Failed to ensure container {} after {} retries", container, max_retries);
    }

    pub async fn head_object(&self, container: &str, object_name: &str) -> Result<bool> {
        let url = self.object_url(container, object_name);
        let max_retries = 5;
        let mut delay = Duration::from_millis(150);

        for attempt in 1..=max_retries {
            let resp = self
                .client
                .head(&url)
                .header("X-Auth-Token", &self.token)
                .send()
                .await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        return Ok(true);
                    } else if status == StatusCode::NOT_FOUND {
                        return Ok(false);
                    }
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        continue;
                    }
                    bail!("HEAD object {} error: {}", object_name, status);
                }
                Err(_) if attempt < max_retries => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "HEAD object {} request failed after {} attempts",
                        object_name, max_retries
                    ));
                }
            }
        }
        bail!("HEAD object {} failed after {} retries", object_name, max_retries);
    }

    pub async fn put_object(&self, container: &str, object_name: &str, data: Vec<u8>) -> Result<()> {
        let url = self.object_url(container, object_name);
        let max_retries = 5;
        let mut delay = Duration::from_millis(200);

        for attempt in 1..=max_retries {
            let resp = self
                .client
                .put(&url)
                .header("X-Auth-Token", &self.token)
                .header("Content-Type", "application/octet-stream")
                .body(data.clone())
                .send()
                .await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() || status == StatusCode::CREATED {
                        return Ok(());
                    }
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        continue;
                    }
                    bail!("PUT object {} failed: {}", object_name, status);
                }
                Err(_) if attempt < max_retries => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "PUT object {} request failed after {} attempts",
                        object_name, max_retries
                    ));
                }
            }
        }
        bail!("PUT object {} failed after {} retries", object_name, max_retries);
    }

    pub async fn get_object(&self, container: &str, object_name: &str) -> Result<Vec<u8>> {
        let url = self.object_url(container, object_name);
        let max_retries = 5;
        let mut delay = Duration::from_millis(200);

        for attempt in 1..=max_retries {
            let resp = self
                .client
                .get(&url)
                .header("X-Auth-Token", &self.token)
                .send()
                .await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        let bytes = r.bytes().await.context("Failed to read body bytes")?;
                        return Ok(bytes.to_vec());
                    }
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        continue;
                    }
                    bail!("GET object {} failed: {}", object_name, status);
                }
                Err(_) if attempt < max_retries => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "GET object {} request failed after {} attempts",
                        object_name, max_retries
                    ));
                }
            }
        }
        bail!("GET object {} failed after {} retries", object_name, max_retries);
    }

    pub async fn delete_object(&self, container: &str, object_name: &str) -> Result<()> {
        let url = self.object_url(container, object_name);
        let max_retries = 5;
        let mut delay = Duration::from_millis(150);

        for attempt in 1..=max_retries {
            let resp = self
                .client
                .delete(&url)
                .header("X-Auth-Token", &self.token)
                .send()
                .await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() || status == StatusCode::NOT_FOUND {
                        return Ok(());
                    }
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries
                    {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                        continue;
                    }
                    bail!("DELETE object {} failed: {}", object_name, status);
                }
                Err(_) if attempt < max_retries => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "DELETE object {} request failed after {} attempts",
                        object_name, max_retries
                    ));
                }
            }
        }
        bail!("DELETE object {} failed after {} retries", object_name, max_retries);
    }

    pub async fn list_objects(&self, container: &str, marker: Option<&str>) -> Result<Vec<String>> {
        let mut url = format!("{}/{}?format=json", self.endpoint, container);
        if let Some(m) = marker {
            url.push_str(&format!("&marker={}", m));
        }

        let resp = self
            .client
            .get(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("GET container listing failed")?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !resp.status().is_success() {
            bail!("Failed to list objects in container {}: {}", container, resp.status());
        }

        #[derive(Deserialize)]
        struct ObjectEntry {
            name: String,
        }

        let entries: Vec<ObjectEntry> = resp.json().await.context("Failed to parse container JSON listing")?;
        Ok(entries.into_iter().map(|e| e.name).collect())
    }

    pub async fn list_all_objects(&self, container: &str) -> Result<Vec<String>> {
        let mut all_objects = Vec::new();
        let mut marker: Option<String> = None;

        loop {
            let page = self.list_objects(container, marker.as_deref()).await?;
            if page.is_empty() {
                break;
            }
            marker = page.last().cloned();
            all_objects.extend(page);
        }

        Ok(all_objects)
    }

    pub async fn delete_container(&self, container: &str) -> Result<()> {
        let url = self.container_url(container);
        let resp = self
            .client
            .delete(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await
            .context("DELETE container request failed")?;

        let status = resp.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            bail!("DELETE container {} failed: {}", container, status);
        }
    }

    pub async fn purge_container(&self, container: &str, concurrency: usize) -> Result<()> {
        let objects = self.list_all_objects(container).await?;
        if !objects.is_empty() {
            let stream = stream::iter(objects).map(|obj| {
                let this = self.clone();
                let cont = container.to_string();
                async move {
                    this.delete_object(&cont, &obj).await
                }
            });
            stream
                .buffer_unordered(concurrency)
                .collect::<Vec<Result<()>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<()>>>()?;
        }
        // Swift container listings/deletions can be eventually consistent.
        // Retry container deletion with backoff if Swift returns 409 Conflict.
        for attempt in 0..5 {
            match self.delete_container(container).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("409") && attempt < 4 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(500 * (1 << attempt))).await;
                        // Re-list and delete any remaining/lingering objects
                        if let Ok(remaining) = self.list_all_objects(container).await {
                            for obj in remaining {
                                let _ = self.delete_object(container, &obj).await;
                            }
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}
