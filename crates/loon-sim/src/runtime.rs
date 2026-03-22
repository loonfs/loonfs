use crate::trace::{SimActorId, SimDelivery, SimTraceEvent};

#[derive(Debug, Default)]
pub struct SimRuntime {
    now_ms: u64,
    next_delivery_id: u64,
    deliveries: Vec<SimDelivery>,
    trace: Vec<SimTraceEvent>,
}

impl SimRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub fn trace(&self) -> &[SimTraceEvent] {
        &self.trace
    }

    pub fn trace_lines(&self) -> Vec<String> {
        self.trace
            .iter()
            .map(SimTraceEvent::render_line)
            .collect::<Vec<_>>()
    }

    pub fn record_actor_step(&mut self, actor_id: impl Into<SimActorId>, step: impl Into<String>) {
        self.trace.push(SimTraceEvent::ActorStep {
            actor_id: actor_id.into(),
            step: step.into(),
            now_ms: self.now_ms,
        });
    }

    pub fn record_restart(&mut self, actor_id: impl Into<SimActorId>) {
        self.trace.push(SimTraceEvent::ActorRestarted {
            actor_id: actor_id.into(),
            now_ms: self.now_ms,
        });
    }

    pub fn record_fault(&mut self, fault: impl Into<String>) {
        self.trace.push(SimTraceEvent::FaultInjected {
            fault: fault.into(),
            now_ms: self.now_ms,
        });
    }

    pub fn advance_time(&mut self, delta_ms: u64) {
        self.now_ms = self.now_ms.saturating_add(delta_ms);
        self.trace.push(SimTraceEvent::TimeAdvanced {
            delta_ms,
            now_ms: self.now_ms,
        });
    }

    pub fn enqueue_delivery(
        &mut self,
        sender: impl Into<SimActorId>,
        recipient: impl Into<SimActorId>,
        kind: impl Into<String>,
    ) -> SimDelivery {
        self.enqueue_delivery_at(sender, recipient, kind, self.now_ms)
    }

    pub fn enqueue_delivery_at(
        &mut self,
        sender: impl Into<SimActorId>,
        recipient: impl Into<SimActorId>,
        kind: impl Into<String>,
        deliver_at_ms: u64,
    ) -> SimDelivery {
        let delivery = SimDelivery {
            delivery_id: self.next_delivery_id.saturating_add(1),
            sender: sender.into(),
            recipient: recipient.into(),
            kind: kind.into(),
            enqueued_at_ms: self.now_ms,
            deliver_at_ms,
        };
        self.next_delivery_id = delivery.delivery_id;
        self.deliveries.push(delivery.clone());
        self.deliveries.sort_by(|left, right| {
            left.deliver_at_ms
                .cmp(&right.deliver_at_ms)
                .then_with(|| left.delivery_id.cmp(&right.delivery_id))
        });
        self.trace.push(SimTraceEvent::DeliveryEnqueued {
            delivery: delivery.clone(),
        });
        delivery
    }

    pub fn pending_deliveries(&self) -> &[SimDelivery] {
        &self.deliveries
    }

    pub fn next_delivery(&self) -> Option<&SimDelivery> {
        self.deliveries.first()
    }

    pub fn take_next_delivery(&mut self) -> Option<SimDelivery> {
        self.take_next_matching_delivery(|_| true)
    }

    pub fn take_next_matching_delivery<F>(&mut self, predicate: F) -> Option<SimDelivery>
    where
        F: Fn(&SimDelivery) -> bool,
    {
        let index = self.deliveries.iter().position(predicate)?;
        let delivery = self.deliveries.remove(index);
        self.now_ms = self.now_ms.max(delivery.deliver_at_ms);
        self.trace.push(SimTraceEvent::DeliveryDelivered {
            delivery: delivery.clone(),
            now_ms: self.now_ms,
        });
        Some(delivery)
    }
}

#[cfg(test)]
mod tests {
    use super::SimRuntime;

    #[test]
    fn runtime_orders_deliveries_by_due_time_then_delivery_id() {
        let mut runtime = SimRuntime::new();
        runtime.enqueue_delivery_at("client", "server", "later", 30);
        runtime.enqueue_delivery_at("client", "server", "first", 10);
        runtime.enqueue_delivery_at("client", "server", "second", 10);

        let first = runtime.take_next_delivery().expect("first delivery");
        let second = runtime.take_next_delivery().expect("second delivery");
        let third = runtime.take_next_delivery().expect("third delivery");

        assert_eq!(first.kind, "first");
        assert_eq!(second.kind, "second");
        assert_eq!(third.kind, "later");
        assert_eq!(runtime.now_ms(), 30);
    }

    #[test]
    fn runtime_renders_stable_trace_lines() {
        let mut runtime = SimRuntime::new();
        runtime.record_actor_step("client", "tick result=enqueued_request request_id=req-1");
        runtime.enqueue_delivery("client", "server", "client_request");
        runtime.record_restart("client");
        runtime.record_fault(
            "fault crash_after_step_once checkpoint_id=dispatch.client.after_response",
        );
        runtime.advance_time(25);
        let delivered = runtime.take_next_delivery().expect("delivery");

        assert_eq!(delivered.delivery_id, 1);
        assert_eq!(
            runtime.trace_lines(),
            vec![
                "actor_step actor=client now_ms=0 tick result=enqueued_request request_id=req-1"
                    .to_owned(),
                "delivery_enqueued delivery_id=1 kind=client_request sender=client recipient=server enqueued_at_ms=0 deliver_at_ms=0"
                    .to_owned(),
                "actor_restarted actor=client now_ms=0".to_owned(),
                "fault_injected now_ms=0 fault crash_after_step_once checkpoint_id=dispatch.client.after_response"
                    .to_owned(),
                "time_advanced delta_ms=25 now_ms=25".to_owned(),
                "delivery_delivered now_ms=25 delivery_id=1 kind=client_request sender=client recipient=server enqueued_at_ms=0 deliver_at_ms=0"
                    .to_owned(),
            ]
        );
    }
}
