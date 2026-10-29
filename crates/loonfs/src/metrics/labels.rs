//! Counters with one series for each value of a closed label vocabulary.

use super::{CounterHandle, MetricsRecorder};
use std::marker::PhantomData;
use std::sync::Arc;

/// A closed vocabulary of values for one metric label.
pub(crate) trait MetricLabel: Copy + PartialEq + 'static {
    const VALUES: &'static [Self];

    fn as_str(self) -> &'static str;

    fn index(self) -> usize {
        Self::VALUES
            .iter()
            .position(|value| *value == self)
            .expect("`MetricLabel::VALUES` should contain every label")
    }
}

#[derive(Clone)]
pub(crate) struct LabeledCounters<E> {
    handles: Vec<Arc<dyn CounterHandle>>,
    marker: PhantomData<fn(E)>,
}

impl<E: MetricLabel> LabeledCounters<E> {
    pub(crate) fn register(
        recorder: &dyn MetricsRecorder,
        name: &'static str,
        description: &'static str,
        label_name: &'static str,
        common_labels: &[(&'static str, &'static str)],
    ) -> Self {
        let handles = E::VALUES
            .iter()
            .map(|value| {
                let mut labels = common_labels.to_vec();
                labels.push((label_name, value.as_str()));
                recorder.register_counter(name, description, &labels)
            })
            .collect();
        Self {
            handles,
            marker: PhantomData,
        }
    }

    pub(crate) fn get(&self, label: E) -> &Arc<dyn CounterHandle> {
        &self.handles[label.index()]
    }
}
