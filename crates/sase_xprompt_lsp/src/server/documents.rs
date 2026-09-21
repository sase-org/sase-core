use super::actions::{detect_recent_paren_insertion, document_eligible};
use super::catalogs::{
    artifact_ref_catalog_signature, glossary_catalog_signature,
    load_artifact_ref_catalog, load_glossary_catalog,
};
use super::state::{
    ArtifactRefCache, ArtifactRefCatalog, ArtifactRefCatalogProject,
    CachedArtifactRefPayload, GlossaryCache, GlossaryCatalog, OpenDocument,
    ServerConfig, XpromptLspServer,
};
use super::*;

impl XpromptLspServer {
    pub(super) fn current_config(&self) -> ServerConfig {
        self.config
            .read()
            .map(|config| config.clone())
            .unwrap_or_default()
    }

    pub(super) fn artifact_ref_catalog(
        &self,
        path: Option<&Path>,
    ) -> ArtifactRefCatalog {
        let signature = artifact_ref_catalog_signature(path);
        let now = Instant::now();
        if let Ok(cache) = self.artifact_ref_cache.read() {
            let fresh = cache.signature.as_ref() == Some(&signature)
                && cache.loaded_at.is_some_and(|loaded_at| {
                    now.saturating_duration_since(loaded_at)
                        < ARTIFACT_REF_CACHE_TTL
                });
            if fresh {
                return cache.catalog.clone();
            }
        }

        let catalog = load_artifact_ref_catalog(path);
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            cache.signature = Some(signature);
            cache.loaded_at = Some(now);
            cache.catalog = catalog.clone();
            cache.payloads.clear();
        }
        catalog
    }

    pub(super) fn glossary_catalog(
        &self,
        path: Option<&Path>,
    ) -> GlossaryCatalog {
        let signature = glossary_catalog_signature(path);
        let now = Instant::now();
        if let Ok(cache) = self.glossary_cache.read() {
            let fresh = cache.signature.as_ref() == Some(&signature)
                && cache.loaded_at.is_some_and(|loaded_at| {
                    now.saturating_duration_since(loaded_at)
                        < GLOSSARY_CACHE_TTL
                });
            if fresh {
                return cache.catalog.clone();
            }
        }

        let catalog = load_glossary_catalog(path);
        if let Ok(mut cache) = self.glossary_cache.write() {
            cache.signature = Some(signature);
            cache.loaded_at = Some(now);
            cache.catalog = catalog.clone();
        }
        catalog
    }

    pub(super) fn cached_at_reference_payload_inventory(
        &self,
        context: &AtReferenceContextWire,
        project: &ArtifactRefCatalogProject,
    ) -> Option<Arc<CachedArtifactRefPayload>> {
        if context.stage != AtReferenceStage::Payload {
            return None;
        }
        let kind = context.kind.as_deref()?;
        if kind == "bug" {
            return None;
        }
        let key = (project.key.clone(), kind.to_string());
        if let Ok(cache) = self.artifact_ref_cache.read() {
            if let Some(payload) = cache.payloads.get(&key) {
                return Some(Arc::clone(payload));
            }
        }

        let Ok(inventory) =
            editor_build_artifact_ref_payload_inventory(kind, &project.context)
        else {
            return None;
        };
        let payload = Arc::new(CachedArtifactRefPayload {
            index: AtReferencePayloadIndex::new(inventory.payloads),
            truncated_payloads: inventory.truncated_payloads,
        });
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            return Some(Arc::clone(
                cache
                    .payloads
                    .entry(key)
                    .or_insert_with(|| Arc::clone(&payload)),
            ));
        }
        Some(payload)
    }

    pub(super) fn invalidate_artifact_ref_cache(&self) {
        if let Ok(mut cache) = self.artifact_ref_cache.write() {
            *cache = ArtifactRefCache::default();
        }
    }

    pub(super) fn invalidate_glossary_cache(&self) {
        if let Ok(mut cache) = self.glossary_cache.write() {
            *cache = GlossaryCache::default();
        }
    }

    pub(super) fn open_document(
        &self,
        uri: &Uri,
        language_id: String,
        text: String,
    ) -> OpenDocument {
        let config = self.current_config();
        OpenDocument {
            eligible: document_eligible(uri, &language_id, &config),
            language_id,
            text,
            recent_paren_insertion: None,
        }
    }

    pub(super) fn changed_document(
        &self,
        uri: &Uri,
        language_id: String,
        text: String,
        previous: Option<&OpenDocument>,
    ) -> OpenDocument {
        let config = self.current_config();
        let recent_paren_insertion = previous.and_then(|document| {
            detect_recent_paren_insertion(
                &document.text,
                &text,
                document.recent_paren_insertion,
            )
        });
        OpenDocument {
            eligible: document_eligible(uri, &language_id, &config),
            language_id,
            text,
            recent_paren_insertion,
        }
    }

    pub(super) fn document_for_uri(&self, uri: &Uri) -> Option<OpenDocument> {
        self.documents
            .read()
            .ok()
            .and_then(|documents| documents.get(&uri.to_string()).cloned())
    }
    pub(super) async fn refresh_catalog_explicit(&self) {
        self.invalidate_artifact_ref_cache();
        self.invalidate_glossary_cache();
        self.catalog_cache.invalidate_agent_catalogs();
        self.catalog_cache.invalidate_finalizer_catalogs();
        let config = self.current_config();
        let xprompt_result = self
            .catalog_cache
            .refresh_explicit(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await;
        let snippet_result = self
            .catalog_cache
            .refresh_snippets_explicit(
                config.catalog_key.clone(),
                config.project.clone(),
                config.root_dir.clone(),
            )
            .await;

        match (xprompt_result, snippet_result) {
            (Ok(entries), Ok(snippets)) => {
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!(
                            "refreshed {} xprompt entries and {} snippets",
                            entries.len(),
                            snippets.len()
                        ),
                    )
                    .await;
            }
            (Ok(entries), Err(snippet_error)) => {
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!("refreshed {} xprompt entries", entries.len()),
                    )
                    .await;
                self.warn_once(&snippet_error).await;
            }
            (Err(xprompt_error), Ok(snippets)) => {
                self.warn_once(&xprompt_error).await;
                self.client
                    .log_message(
                        MessageType::INFO,
                        format!("refreshed {} snippets", snippets.len()),
                    )
                    .await;
            }
            (Err(xprompt_error), Err(snippet_error)) => {
                self.warn_once(&xprompt_error).await;
                self.warn_once(&snippet_error).await;
            }
        }
        self.request_semantic_tokens_refresh();
    }

    pub(super) async fn publish_document_diagnostics(
        &self,
        uri: Uri,
        document: OpenDocument,
    ) {
        let diagnostics = if document.eligible {
            self.diagnostics_for_uri_text(&uri, document.text).await
        } else {
            Vec::new()
        };
        self.client
            .publish_diagnostics(uri, diagnostics, None)
            .await;
    }

    pub(super) async fn warn_once(&self, error: &CatalogFailure) {
        warn!("{}", error.message);
        if self.catalog_cache.should_warn(&error.class) {
            self.client
                .show_message(MessageType::WARNING, error.message.clone())
                .await;
        }
    }

    pub(super) fn request_semantic_tokens_refresh(&self) {
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.semantic_tokens_refresh().await;
        });
    }
}
