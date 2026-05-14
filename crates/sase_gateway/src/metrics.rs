use std::{
    fmt::Write as _,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

const LATENCY_BUCKETS_SECONDS: [f64; 8] =
    [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

#[derive(Clone, Debug)]
pub struct DaemonMetrics {
    inner: Arc<DaemonMetricsInner>,
}

#[derive(Debug)]
struct DaemonMetricsInner {
    active_connections: Arc<AtomicI64>,
    rpc_requests_total: AtomicU64,
    rpc_errors_total: AtomicU64,
    rpc_payload_rejections_total: AtomicU64,
    subscription_count: Arc<AtomicI64>,
    dropped_events_total: AtomicU64,
    health_ok: AtomicU64,
    health_degraded: AtomicU64,
    indexing_queued_changes_total: AtomicU64,
    indexing_dropped_changes_total: AtomicU64,
    indexing_coalesced_changes_total: AtomicU64,
    indexing_indexed_sources_total: AtomicU64,
    indexing_failed_parses_total: AtomicU64,
    indexing_shadow_diff_missing_total: AtomicU64,
    indexing_shadow_diff_stale_total: AtomicU64,
    indexing_shadow_diff_extra_total: AtomicU64,
    indexing_shadow_diff_corrupt_total: AtomicU64,
    scheduler_batch_submits_total: AtomicU64,
    scheduler_status_queries_total: AtomicU64,
    scheduler_cancels_total: AtomicU64,
    scheduler_recovery_repairs_total: AtomicU64,
    rpc_latency: LatencyHistogram,
    projection_query_latency: LatencyHistogram,
    projection_event_append_latency: LatencyHistogram,
    scheduler_batch_submit_latency: LatencyHistogram,
    scheduler_status_query_latency: LatencyHistogram,
    scheduler_cancel_latency: LatencyHistogram,
}

#[derive(Debug)]
struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_SECONDS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

#[derive(Debug)]
pub struct GaugeGuard {
    value: Arc<AtomicI64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexingMetricsReport {
    pub indexed_sources: u64,
    pub failed_parses: u64,
    pub coalesced_changes: u64,
    pub missing: u64,
    pub stale: u64,
    pub extra: u64,
    pub corrupt: u64,
}

impl Default for DaemonMetrics {
    fn default() -> Self {
        Self {
            inner: Arc::new(DaemonMetricsInner {
                active_connections: Arc::new(AtomicI64::new(0)),
                rpc_requests_total: AtomicU64::new(0),
                rpc_errors_total: AtomicU64::new(0),
                rpc_payload_rejections_total: AtomicU64::new(0),
                subscription_count: Arc::new(AtomicI64::new(0)),
                dropped_events_total: AtomicU64::new(0),
                health_ok: AtomicU64::new(1),
                health_degraded: AtomicU64::new(0),
                indexing_queued_changes_total: AtomicU64::new(0),
                indexing_dropped_changes_total: AtomicU64::new(0),
                indexing_coalesced_changes_total: AtomicU64::new(0),
                indexing_indexed_sources_total: AtomicU64::new(0),
                indexing_failed_parses_total: AtomicU64::new(0),
                indexing_shadow_diff_missing_total: AtomicU64::new(0),
                indexing_shadow_diff_stale_total: AtomicU64::new(0),
                indexing_shadow_diff_extra_total: AtomicU64::new(0),
                indexing_shadow_diff_corrupt_total: AtomicU64::new(0),
                scheduler_batch_submits_total: AtomicU64::new(0),
                scheduler_status_queries_total: AtomicU64::new(0),
                scheduler_cancels_total: AtomicU64::new(0),
                scheduler_recovery_repairs_total: AtomicU64::new(0),
                rpc_latency: LatencyHistogram::new(),
                projection_query_latency: LatencyHistogram::new(),
                projection_event_append_latency: LatencyHistogram::new(),
                scheduler_batch_submit_latency: LatencyHistogram::new(),
                scheduler_status_query_latency: LatencyHistogram::new(),
                scheduler_cancel_latency: LatencyHistogram::new(),
            }),
        }
    }
}

impl DaemonMetrics {
    pub fn connection_guard(&self) -> GaugeGuard {
        self.inner
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
        GaugeGuard {
            value: Arc::clone(&self.inner.active_connections),
        }
    }

    pub fn subscription_guard(&self) -> GaugeGuard {
        self.inner
            .subscription_count
            .fetch_add(1, Ordering::Relaxed);
        GaugeGuard {
            value: Arc::clone(&self.inner.subscription_count),
        }
    }

    pub fn record_rpc(&self, duration: Duration, success: bool) {
        self.inner
            .rpc_requests_total
            .fetch_add(1, Ordering::Relaxed);
        if !success {
            self.inner.rpc_errors_total.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.rpc_latency.observe(duration);
    }

    pub fn record_payload_rejection(&self) {
        self.inner
            .rpc_payload_rejections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_projection_query(&self, duration: Duration) {
        self.inner.projection_query_latency.observe(duration);
    }

    pub fn record_projection_event_append(&self, duration: Duration) {
        self.inner.projection_event_append_latency.observe(duration);
    }

    pub fn record_dropped_event(&self) {
        self.inner
            .dropped_events_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_scheduler_batch_submit(&self, duration: Duration) {
        self.inner
            .scheduler_batch_submits_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner.scheduler_batch_submit_latency.observe(duration);
    }

    pub fn record_scheduler_status_query(&self, duration: Duration) {
        self.inner
            .scheduler_status_queries_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner.scheduler_status_query_latency.observe(duration);
    }

    pub fn record_scheduler_cancel(&self, duration: Duration) {
        self.inner
            .scheduler_cancels_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner.scheduler_cancel_latency.observe(duration);
    }

    pub fn record_scheduler_recovery_repairs(&self, count: u64) {
        self.inner
            .scheduler_recovery_repairs_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_indexing_queued_change(&self) {
        self.inner
            .indexing_queued_changes_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_indexing_dropped_change(&self) {
        self.inner
            .indexing_dropped_changes_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_indexing_report(&self, report: IndexingMetricsReport) {
        self.inner
            .indexing_indexed_sources_total
            .fetch_add(report.indexed_sources, Ordering::Relaxed);
        self.inner
            .indexing_failed_parses_total
            .fetch_add(report.failed_parses, Ordering::Relaxed);
        self.inner
            .indexing_coalesced_changes_total
            .fetch_add(report.coalesced_changes, Ordering::Relaxed);
        self.inner
            .indexing_shadow_diff_missing_total
            .fetch_add(report.missing, Ordering::Relaxed);
        self.inner
            .indexing_shadow_diff_stale_total
            .fetch_add(report.stale, Ordering::Relaxed);
        self.inner
            .indexing_shadow_diff_extra_total
            .fetch_add(report.extra, Ordering::Relaxed);
        self.inner
            .indexing_shadow_diff_corrupt_total
            .fetch_add(report.corrupt, Ordering::Relaxed);
    }

    pub fn set_health_ok(&self, ok: bool) {
        self.inner.health_ok.store(u64::from(ok), Ordering::Relaxed);
        self.inner
            .health_degraded
            .store(u64::from(!ok), Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        metric_type(&mut out, "sase_daemon_active_connections", "gauge");
        metric_value(
            &mut out,
            "sase_daemon_active_connections",
            self.inner.active_connections.load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_rpc_requests_total", "counter");
        metric_value(
            &mut out,
            "sase_daemon_rpc_requests_total",
            self.inner.rpc_requests_total.load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_rpc_errors_total", "counter");
        metric_value(
            &mut out,
            "sase_daemon_rpc_errors_total",
            self.inner.rpc_errors_total.load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_rpc_payload_rejections_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_rpc_payload_rejections_total",
            self.inner
                .rpc_payload_rejections_total
                .load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_subscriptions", "gauge");
        metric_value(
            &mut out,
            "sase_daemon_subscriptions",
            self.inner.subscription_count.load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_dropped_events_total", "counter");
        metric_value(
            &mut out,
            "sase_daemon_dropped_events_total",
            self.inner.dropped_events_total.load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_queued_changes_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_indexing_queued_changes_total",
            self.inner
                .indexing_queued_changes_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_dropped_changes_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_indexing_dropped_changes_total",
            self.inner
                .indexing_dropped_changes_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_coalesced_changes_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_indexing_coalesced_changes_total",
            self.inner
                .indexing_coalesced_changes_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_indexed_sources_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_indexing_indexed_sources_total",
            self.inner
                .indexing_indexed_sources_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_failed_parses_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_indexing_failed_parses_total",
            self.inner
                .indexing_failed_parses_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_indexing_shadow_diffs_total",
            "counter",
        );
        labeled_metric_value(
            &mut out,
            "sase_daemon_indexing_shadow_diffs_total",
            "category",
            "missing",
            self.inner
                .indexing_shadow_diff_missing_total
                .load(Ordering::Relaxed),
        );
        labeled_metric_value(
            &mut out,
            "sase_daemon_indexing_shadow_diffs_total",
            "category",
            "stale",
            self.inner
                .indexing_shadow_diff_stale_total
                .load(Ordering::Relaxed),
        );
        labeled_metric_value(
            &mut out,
            "sase_daemon_indexing_shadow_diffs_total",
            "category",
            "extra",
            self.inner
                .indexing_shadow_diff_extra_total
                .load(Ordering::Relaxed),
        );
        labeled_metric_value(
            &mut out,
            "sase_daemon_indexing_shadow_diffs_total",
            "category",
            "corrupt",
            self.inner
                .indexing_shadow_diff_corrupt_total
                .load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_health_status", "gauge");
        labeled_metric_value(
            &mut out,
            "sase_daemon_health_status",
            "status",
            "ok",
            self.inner.health_ok.load(Ordering::Relaxed),
        );
        labeled_metric_value(
            &mut out,
            "sase_daemon_health_status",
            "status",
            "degraded",
            self.inner.health_degraded.load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_scheduler_batch_submits_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_scheduler_batch_submits_total",
            self.inner
                .scheduler_batch_submits_total
                .load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_scheduler_status_queries_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_scheduler_status_queries_total",
            self.inner
                .scheduler_status_queries_total
                .load(Ordering::Relaxed),
        );
        metric_type(&mut out, "sase_daemon_scheduler_cancels_total", "counter");
        metric_value(
            &mut out,
            "sase_daemon_scheduler_cancels_total",
            self.inner.scheduler_cancels_total.load(Ordering::Relaxed),
        );
        metric_type(
            &mut out,
            "sase_daemon_scheduler_recovery_repairs_total",
            "counter",
        );
        metric_value(
            &mut out,
            "sase_daemon_scheduler_recovery_repairs_total",
            self.inner
                .scheduler_recovery_repairs_total
                .load(Ordering::Relaxed),
        );
        self.inner
            .rpc_latency
            .render(&mut out, "sase_daemon_rpc_latency_seconds");
        self.inner
            .projection_query_latency
            .render(&mut out, "sase_daemon_projection_query_latency_seconds");
        self.inner.projection_event_append_latency.render(
            &mut out,
            "sase_daemon_projection_event_append_latency_seconds",
        );
        self.inner.scheduler_batch_submit_latency.render(
            &mut out,
            "sase_daemon_scheduler_batch_submit_latency_seconds",
        );
        self.inner.scheduler_status_query_latency.render(
            &mut out,
            "sase_daemon_scheduler_status_query_latency_seconds",
        );
        self.inner
            .scheduler_cancel_latency
            .render(&mut out, "sase_daemon_scheduler_cancel_latency_seconds");
        out
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    fn observe(&self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        for (idx, bucket) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            if seconds <= *bucket {
                self.buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn render(&self, out: &mut String, name: &str) {
        metric_type(out, name, "histogram");
        for (idx, bucket) in LATENCY_BUCKETS_SECONDS.iter().enumerate() {
            let value = self.buckets[idx].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bucket}\"}} {value}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

fn metric_type(out: &mut String, name: &str, kind: &str) {
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn metric_value(out: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "{name} {value}");
}

fn labeled_metric_value(
    out: &mut String,
    name: &str,
    label: &str,
    label_value: &str,
    value: impl std::fmt::Display,
) {
    let _ = writeln!(out, "{name}{{{label}=\"{label_value}\"}} {value}");
}
