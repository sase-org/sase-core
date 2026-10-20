//! Active-train selection. Newest generation stays active until confirmed
//! or abandoned; historical pre-train tags are not unfinished trains.

use super::facts::TrainFactsWire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainSelection {
    pub selected: Option<TrainFactsWire>,
    pub confirmed: Option<TrainFactsWire>,
    pub active_versions: Vec<String>,
    pub ambiguous: bool,
    pub mismatch: Option<String>,
}

pub fn select_trains(trains: &[TrainFactsWire]) -> TrainSelection {
    for train in trains {
        if train.manifest_mismatch {
            return TrainSelection {
                selected: None,
                confirmed: None,
                active_versions: Vec::new(),
                ambiguous: false,
                mismatch: Some(format!(
                    "tag/manifest mismatch for {}",
                    train.candidate_id
                )),
            };
        }
        if train.evidence_mismatch {
            return TrainSelection {
                selected: None,
                confirmed: None,
                active_versions: Vec::new(),
                ambiguous: false,
                mismatch: Some(format!(
                    "generation evidence mismatch for {}",
                    train.candidate_id
                )),
            };
        }
    }

    let mut latest: Vec<TrainFactsWire> = Vec::new();
    for train in trains {
        if train.pre_train_legacy {
            continue;
        }
        match latest.iter_mut().find(|item| {
            item.candidate_id == train.candidate_id
                || item.version == train.version
        }) {
            Some(existing) => {
                if train.generation >= existing.generation {
                    *existing = train.clone();
                }
            }
            None => latest.push(train.clone()),
        }
    }

    let confirmed = latest
        .iter()
        .filter(|train| train.confirmed && !train.abandoned)
        .max_by_key(|train| train.generation)
        .cloned();
    let active: Vec<TrainFactsWire> = latest
        .into_iter()
        .filter(|train| !train.confirmed && !train.abandoned)
        .collect();
    let active_versions: Vec<String> =
        active.iter().map(|train| train.version.clone()).collect();
    if active.len() > 1 {
        return TrainSelection {
            selected: None,
            confirmed,
            active_versions,
            ambiguous: true,
            mismatch: None,
        };
    }
    TrainSelection {
        selected: active.into_iter().next(),
        confirmed,
        active_versions,
        ambiguous: false,
        mismatch: None,
    }
}

pub fn version_has_pypi_files(train: &TrainFactsWire) -> bool {
    train.py_files_present > 0
}
