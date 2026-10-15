pub mod adapt;
pub mod client;
pub mod crypto;
pub mod session;
pub mod title;

pub use title::clean_moviebox_title;

use crate::providers::models::{CatalogItem, MediaDetails, ProviderError, ProviderKind};
use crate::providers::{Provider, ProviderCapabilities};
use client::{MovieBoxClient, ScraperError};
use serde_json::{Value, json};

pub const STREAM_REFERER: &str = "https://sportslive.wine";

impl From<ScraperError> for ProviderError {
    fn from(err: ScraperError) -> Self {
        match err {
            ScraperError::Reqwest(e) => ProviderError::Network(e.to_string()),
            ScraperError::ApiStatus(429) => ProviderError::RateLimited(None),
            ScraperError::ApiStatus(404) => ProviderError::NotFound,
            ScraperError::ApiStatus(s) => ProviderError::Unavailable(format!("HTTP status {s}")),
            ScraperError::ApiCode { code: 429, .. } => ProviderError::RateLimited(None),
            ScraperError::ApiCode { code: 404, .. } => ProviderError::NotFound,
            ScraperError::ApiCode { code, message } => {
                ProviderError::Unavailable(format!("API error {code}: {message}"))
            }
            ScraperError::HostsExhausted => {
                ProviderError::Unavailable("All hosts exhausted".to_string())
            }
            ScraperError::Json(e) => ProviderError::Parsing(e.to_string()),
            ScraperError::MissingToken => ProviderError::Unavailable("Missing token".to_string()),
        }
    }
}

impl Provider for client::MovieBoxClient {
    fn id(&self) -> ProviderKind {
        ProviderKind::MovieBox
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_search: true,
            supports_pagination: true,
            supports_series: true,
            supports_subtitles: true,
            supports_homepage: true,
        }
    }

    async fn search(&self, query: &str, page: usize) -> Result<Vec<CatalogItem>, ProviderError> {
        let json = self
            .search(query, page)
            .await
            .map_err(ProviderError::from)?;
        Ok(adapt::moviebox_search_json_to_catalog(&json))
    }

    async fn details(&self, id: &str) -> Result<MediaDetails, ProviderError> {
        let json = self.get_details(id).await.map_err(ProviderError::from)?;
        adapt::moviebox_details_json_to_media_details(&json)
    }
}

impl crate::providers::ReleaseProvider for client::MovieBoxClient {
    async fn episode_streams(
        &self,
        id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<crate::providers::models::Release>, ProviderError> {
        let (play_info_res, resources_res) = tokio::join!(
            self.get_play_info(id, season, episode),
            self.get_resources(
                id,
                season,
                episode,
                if episode > 0 {
                    (episode - 1) / 20 + 1
                } else {
                    1
                },
                None,
                20,
            )
        );

        merge_stream_responses(
            play_info_res,
            resources_res,
            season,
            episode,
            self.user_agent(),
        )
    }
}

fn merge_stream_responses(
    play_info_res: Result<Value, ScraperError>,
    resources_res: Result<Value, ScraperError>,
    season: usize,
    episode: usize,
    user_agent: &str,
) -> Result<Vec<crate::providers::models::Release>, ProviderError> {
    let mut releases = Vec::new();
    let mut seen_urls = std::collections::HashSet::new();
    let mut endpoint_errors = Vec::new();

    match play_info_res {
        Ok(json) => {
            for release in
                adapt::moviebox_play_info_json_to_releases(&json, season, episode, user_agent)
            {
                if let Some(url) = release.direct_url() {
                    let base = url.split('?').next().unwrap_or(url).to_string();
                    if !base.is_empty() {
                        seen_urls.insert(base);
                    }
                }
                releases.push(release);
            }
        }
        Err(error) => {
            log::warn!("MovieBox play-info request failed: {error}");
            endpoint_errors.push(("play-info", error));
        }
    }

    match resources_res {
        Ok(json) => {
            for release in adapt::moviebox_resource_json_to_releases(&json) {
                if let Some(url) = release.direct_url() {
                    let base = url.split('?').next().unwrap_or(url).to_string();
                    if !base.is_empty() && seen_urls.contains(&base) {
                        continue;
                    }
                    seen_urls.insert(base);
                }
                if (season == 0 && episode == 0)
                    || (release.season == Some(season) && release.episode == Some(episode))
                    || (release.season.is_none() && release.episode.is_none())
                {
                    releases.push(release);
                }
            }
        }
        Err(error) => {
            log::warn!("MovieBox resource request failed: {error}");
            endpoint_errors.push(("resource", error));
        }
    }

    if releases.is_empty() {
        if endpoint_errors.is_empty() {
            return Err(ProviderError::Unavailable(
                "No stream sources available".to_string(),
            ));
        }
        return Err(stream_endpoint_error(endpoint_errors));
    }

    releases.sort_by(|left, right| {
        right
            .resolution_u64()
            .cmp(&left.resolution_u64())
            .then_with(|| right.size_bytes.cmp(&left.size_bytes))
    });

    Ok(releases)
}

fn stream_endpoint_error(errors: Vec<(&'static str, ScraperError)>) -> ProviderError {
    let mut errors = errors.into_iter();
    let Some((first_endpoint, first_error)) = errors.next() else {
        return ProviderError::Unavailable("No stream sources available".to_string());
    };

    if let Some((second_endpoint, second_error)) = errors.next() {
        return ProviderError::Unavailable(format!(
            "MovieBox stream lookup failed ({first_endpoint}: {first_error}; {second_endpoint}: {second_error})"
        ));
    }

    first_error.into()
}

impl MovieBoxClient {
    pub async fn get_play_info(
        &self,
        subject_id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Value, ScraperError> {
        let path = if season == 0 && episode == 0 {
            format!(
                "/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={}",
                subject_id
            )
        } else {
            format!(
                "/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={}&se={}&ep={}",
                subject_id, season, episode
            )
        };
        self.get(&path).await
    }
    pub async fn search(&self, query: &str, page: usize) -> Result<Value, ScraperError> {
        let payload = json!({
            "keyword": query,
            "page": page,
            "perPage": 15,
            "subjectType": 0
        });
        self.post("/wefeed-mobile-bff/subject-api/search/v2", &payload)
            .await
    }

    pub async fn suggest(&self, query: &str) -> Result<Value, ScraperError> {
        self.search(query, 1).await
    }

    pub async fn get_details(&self, subject_id: &str) -> Result<Value, ScraperError> {
        let path = format!(
            "/wefeed-mobile-bff/subject-api/get?subjectId={}",
            subject_id
        );
        let mut details = self.get(&path).await?;

        let stype = details
            .get("subjectType")
            .and_then(|s| s.as_i64())
            .or_else(|| details.get("stype").and_then(|s| s.as_i64()))
            .unwrap_or(1);

        if stype == 2 {
            let season_path = format!(
                "/wefeed-mobile-bff/subject-api/season-info?subjectId={}",
                subject_id
            );
            if let Ok(season_info) = self.get(&season_path).await {
                if let Value::Object(ref mut map) = details {
                    map.insert("seasons".to_string(), season_info);
                }
            }
        }

        Ok(details)
    }

    pub async fn get_homepage(&self, tab_id: &str, page: usize) -> Result<Value, ScraperError> {
        let path = format!(
            "/wefeed-mobile-bff/tab-operating?page={}&tabId={}&version=",
            page, tab_id
        );
        self.get(&path).await
    }

    pub async fn get_resources(
        &self,
        subject_id: &str,
        season: usize,
        episode: usize,
        page: usize,
        resolution: Option<&str>,
        per_page: usize,
    ) -> Result<Value, ScraperError> {
        let res_param = if let Some(r) = resolution {
            if r.is_empty() {
                String::new()
            } else {
                format!("&resolution={}", r)
            }
        } else {
            String::new()
        };

        let path = if season == 0 && episode == 0 {
            format!(
                "/wefeed-mobile-bff/subject-api/resource?subjectId={}&page={}&perPage={}{}",
                subject_id, page, per_page, res_param
            )
        } else {
            format!(
                "/wefeed-mobile-bff/subject-api/resource?subjectId={}&se={}&ep={}&page={}&perPage={}{}",
                subject_id, season, episode, page, per_page, res_param
            )
        };
        self.get(&path).await
    }

    pub async fn fetch_resource_page(
        &self,
        subject_id: &str,
        season: usize,
        episode: usize,
        resolution: u32,
        page: usize,
    ) -> Result<(Vec<Value>, Value), ScraperError> {
        let path = resource_page_path(subject_id, season, episode, resolution, page);

        let res = self.get(&path).await?;

        let items = res
            .get("list")
            .and_then(|l| l.as_array())
            .cloned()
            .unwrap_or_default();

        let pager = res.get("pager").cloned().unwrap_or_else(|| json!({}));

        Ok((items, pager))
    }

    pub async fn fetch_collection_resolutions(
        &self,
        subject_id: &str,
    ) -> Result<Vec<u32>, ScraperError> {
        let path = format!(
            "/wefeed-mobile-bff/subject-api/resource?subjectId={}&page=1&perPage=20",
            subject_id
        );
        let res = self.get(&path).await?;

        let mut resolutions = Vec::new();
        if let Some(cols) = res.get("collectionResolutions").and_then(|c| c.as_array()) {
            for col in cols {
                if let Some(r) = col.get("resolution").and_then(|v| v.as_u64()) {
                    if r > 0 {
                        resolutions.push(r as u32);
                    }
                }
            }
        }

        if resolutions.is_empty() {
            if let Some(list) = res.get("list").and_then(|l| l.as_array()) {
                for item in list {
                    if let Some(r) = item.get("resolution").and_then(|v| v.as_u64()) {
                        let res_u32 = r as u32;
                        if res_u32 > 0 && !resolutions.contains(&res_u32) {
                            resolutions.push(res_u32);
                        }
                    }
                }
            }
        }

        resolutions.sort_by(|a, b| b.cmp(a));

        if resolutions.is_empty() {
            resolutions = vec![1080, 720, 480, 360];
        }
        Ok(resolutions)
    }

    pub async fn get_ext_captions(
        &self,
        subject_id: &str,
        resource_id: &str,
    ) -> Result<Value, ScraperError> {
        let path = format!(
            "/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={}&resourceId={}",
            subject_id, resource_id
        );
        self.get(&path).await
    }
}

pub fn resource_page_path(
    subject_id: &str,
    season: usize,
    episode: usize,
    resolution: u32,
    page: usize,
) -> String {
    let res_param = if resolution == 0 {
        String::new()
    } else {
        format!("&resolution={}", resolution)
    };

    if season == 0 && episode == 0 {
        format!(
            "/wefeed-mobile-bff/subject-api/resource?subjectId={}&page={}&perPage=20{}",
            subject_id, page, res_param
        )
    } else {
        format!(
            "/wefeed-mobile-bff/subject-api/resource?subjectId={}&se={}&ep={}&page={}&perPage=20{}",
            subject_id, season, episode, page, res_param
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_page_path_movie() {
        let path = resource_page_path("12345", 0, 0, 0, 1);
        assert_eq!(
            path,
            "/wefeed-mobile-bff/subject-api/resource?subjectId=12345&page=1&perPage=20"
        );
    }

    #[test]
    fn test_resource_page_path_series_with_se_and_ep() {
        let path = resource_page_path("67890", 2, 5, 0, 1);
        assert_eq!(
            path,
            "/wefeed-mobile-bff/subject-api/resource?subjectId=67890&se=2&ep=5&page=1&perPage=20"
        );
    }

    #[test]
    fn test_resource_page_path_with_resolution() {
        let path = resource_page_path("67890", 1, 10, 1080, 2);
        assert_eq!(
            path,
            "/wefeed-mobile-bff/subject-api/resource?subjectId=67890&se=1&ep=10&page=2&perPage=20&resolution=1080"
        );
    }

    #[test]
    fn test_failed_moviebox_stream_request_is_not_misreported_as_no_streams() {
        let error = merge_stream_responses(
            Err(ScraperError::ApiStatus(502)),
            Ok(json!({ "list": [] })),
            0,
            0,
            "TestAgent/1.0",
        )
        .expect_err("failed play-info request should be surfaced");

        assert!(error.to_string().contains("HTTP status 502"));
        assert!(!error.to_string().contains("No stream sources available"));
    }

    #[test]
    fn test_both_moviebox_stream_endpoint_errors_are_reported_together() {
        let error = merge_stream_responses(
            Err(ScraperError::HostsExhausted),
            Err(ScraperError::ApiStatus(502)),
            0,
            0,
            "TestAgent/1.0",
        )
        .expect_err("both endpoint failures should be surfaced");

        let message = error.to_string();
        assert!(message.contains("play-info: All hosts exhausted"));
        assert!(message.contains("resource: API error status: 502"));
        assert!(!message.contains("No stream sources available"));
    }

    #[test]
    fn test_empty_successful_moviebox_responses_report_no_streams() {
        let error = merge_stream_responses(
            Ok(json!({ "streams": [] })),
            Ok(json!({ "list": [] })),
            0,
            0,
            "TestAgent/1.0",
        )
        .expect_err("empty successful responses should report an empty result");

        assert!(error.to_string().contains("No stream sources available"));
    }

    #[test]
    fn test_resource_streams_are_kept_when_play_info_fails() {
        let result = merge_stream_responses(
            Err(ScraperError::HostsExhausted),
            Ok(json!({
                "list": [{
                    "resourceId": "resource-1",
                    "fileName": "Example Movie 1080p",
                    "resolution": 1080,
                    "resourceLink": "https://cdn.example.com/example.mp4"
                }]
            })),
            0,
            0,
            "TestAgent/1.0",
        )
        .expect("valid resource links should remain usable when play-info fails");

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].direct_url(),
            Some("https://cdn.example.com/example.mp4")
        );
    }
}
