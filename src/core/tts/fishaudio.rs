use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use xxhash_rust::xxh3::xxh3_128;

use super::base::{AudioCallback, BaseTTS, ConnectionState, TTSConfig, TTSResult};
use super::provider::{PronunciationReplacer, TTSProvider, TTSRequestBuilder};
use crate::utils::req_manager::ReqManager;

/// Fish Audio HTTP TTS endpoint
pub const FISHAUDIO_TTS_URL: &str = "https://api.fish.audio/v1/tts";

/// Default Fish Audio model matching Marvin's `settings.FISH_AUDIO_MODEL`
pub const DEFAULT_FISHAUDIO_MODEL: &str = "s2.1-pro";

#[derive(Clone)]
struct FishAudioRequestBuilder {
    config: TTSConfig,
    pronunciation_replacer: Option<PronunciationReplacer>,
}

impl TTSRequestBuilder for FishAudioRequestBuilder {
    fn build_http_request(&self, client: &reqwest::Client, text: &str) -> reqwest::RequestBuilder {
        // In Marvin's AgentConfig, `voice_model` holds the Fish Audio reference_id (voice_id),
        // while the Fish Audio model header is `s2.1-pro`. Support passing the reference_id
        // in either `voice_id` or `model`.
        let (fish_model, reference_id) = match (
            self.config.voice_id.as_deref().filter(|s| !s.is_empty()),
            self.config.model.as_str(),
        ) {
            (Some(vid), m) if !m.is_empty() && m != vid => (m, vid),
            (Some(vid), _) => (DEFAULT_FISHAUDIO_MODEL, vid),
            (None, m) if !m.is_empty() => (DEFAULT_FISHAUDIO_MODEL, m),
            (None, _) => (DEFAULT_FISHAUDIO_MODEL, "933563129e564b19a115bedd57b7406a"),
        };

        let sample_rate = self.config.sample_rate.unwrap_or(24000);

        client
            .post(FISHAUDIO_TTS_URL)
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json")
            .header("model", fish_model)
            .json(&json!({
                "text": text,
                "chunk_length": 100,
                "format": "pcm",
                "sample_rate": sample_rate,
                "reference_id": reference_id,
                "normalize": true,
                "latency": "balanced",
                "features": ["quality-guard"]
            }))
    }

    fn get_config(&self) -> &TTSConfig {
        &self.config
    }

    fn get_pronunciation_replacer(&self) -> Option<&PronunciationReplacer> {
        self.pronunciation_replacer.as_ref()
    }
}

fn compute_tts_config_hash(config: &TTSConfig) -> String {
    let mut s = String::new();
    s.push_str("fishaudio|");
    s.push_str(config.voice_id.as_deref().unwrap_or(""));
    s.push('|');
    s.push_str(&config.model);
    s.push('|');
    s.push_str(&config.sample_rate.unwrap_or(24000).to_string());
    let hash = xxh3_128(s.as_bytes());
    format!("{hash:032x}")
}

pub struct FishAudioTTS {
    provider: TTSProvider,
    request_builder: FishAudioRequestBuilder,
    config_hash: String,
}

impl FishAudioTTS {
    pub fn new(mut config: TTSConfig) -> TTSResult<Self> {
        // Fish Audio PCM defaults to 24000Hz mono 16-bit PCM.
        if config.sample_rate.is_none() {
            config.sample_rate = Some(24000);
        }
        if config.audio_format.is_none() {
            config.audio_format = Some("linear16".to_string());
        }
        let pronunciation_replacer = if !config.pronunciations.is_empty() {
            Some(PronunciationReplacer::new(&config.pronunciations))
        } else {
            None
        };
        let config_hash = compute_tts_config_hash(&config);
        let request_builder = FishAudioRequestBuilder {
            config,
            pronunciation_replacer,
        };
        Ok(Self {
            provider: TTSProvider::new()?,
            request_builder,
            config_hash,
        })
    }

    pub async fn set_req_manager(&mut self, req_manager: Arc<ReqManager>) {
        self.provider.set_req_manager(req_manager).await;
    }
}

#[async_trait]
impl BaseTTS for FishAudioTTS {
    fn new(config: TTSConfig) -> TTSResult<Self> {
        FishAudioTTS::new(config)
    }

    fn get_provider(&mut self) -> Option<&mut TTSProvider> {
        Some(&mut self.provider)
    }

    async fn connect(&mut self) -> TTSResult<()> {
        self.provider
            .generic_connect_with_config(FISHAUDIO_TTS_URL, &self.request_builder.config)
            .await
    }

    async fn disconnect(&mut self) -> TTSResult<()> {
        self.provider.generic_disconnect().await
    }

    fn is_ready(&self) -> bool {
        self.provider.is_ready()
    }

    fn get_connection_state(&self) -> ConnectionState {
        self.provider.get_connection_state()
    }

    async fn speak(&mut self, text: &str, flush: bool) -> TTSResult<()> {
        if !self.is_ready() {
            self.connect().await?;
        }
        self.provider
            .set_tts_config_hash(self.config_hash.clone())
            .await;
        self.provider
            .generic_speak(self.request_builder.clone(), text, flush)
            .await
    }

    async fn clear(&mut self) -> TTSResult<()> {
        self.provider.generic_clear().await
    }

    async fn flush(&self) -> TTSResult<()> {
        self.provider.generic_flush().await
    }

    fn on_audio(&mut self, callback: Arc<dyn AudioCallback>) -> TTSResult<()> {
        self.provider.generic_on_audio(callback)
    }

    fn remove_audio_callback(&mut self) -> TTSResult<()> {
        self.provider.generic_remove_audio_callback()
    }

    fn get_provider_info(&self) -> serde_json::Value {
        serde_json::json!({
            "provider": "fishaudio",
            "version": "1.0.0",
            "api_type": "HTTP REST",
            "endpoint": FISHAUDIO_TTS_URL,
            "supported_formats": ["pcm"],
            "supported_sample_rates": [24000],
        })
    }
}
