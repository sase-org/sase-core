//! Provider-usage tests, split along the same seams as the production
//! modules so each test file sits beside the code it covers.

mod admission_policy;
mod agy;
mod attempt_policy;
mod attention;
mod compatibility;
mod freshness_health;
mod indicator;
mod muse;
mod refresh;
mod snapshot_projection;
mod store;
mod support;
mod validation;
