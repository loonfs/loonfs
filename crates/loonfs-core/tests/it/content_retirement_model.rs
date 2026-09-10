//! Independent exploration of namespace retirement and fork record deletion.
//! The deadline is measured from the call clock. The budget bounds how far
//! behind that clock can be at the moment of publication.

use std::collections::HashSet;

const BUDGET: u64 = 1;
const CAPABILITY_LIFETIME: u64 = 2;
const GRACE: u64 = BUDGET + CAPABILITY_LIFETIME;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Status {
    Active,
    Deleted,
    Retired { deadline: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Record {
    source: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Model {
    namespaces: [Option<Status>; 3],
    records: [Option<Record>; 3],
    collectable: [bool; 3],
    inherited_owners: [u8; 3],
    capabilities_until: [u64; 3],
    calls: [Option<u64>; 3],
    clock: u64,
}

#[derive(Clone, Copy)]
enum Rule {
    Correct,
    ReleaseUnretired,
    Backdate,
}

#[derive(Clone, Copy)]
enum Action {
    Fork(usize, usize),
    Issue(usize),
    Delete(usize),
    Start(usize),
    Gc(usize),
    Advance,
}

impl Model {
    fn new() -> Self {
        Self {
            namespaces: [Some(Status::Active), None, None],
            records: [None; 3],
            collectable: [false; 3],
            inherited_owners: [1, 2, 4],
            capabilities_until: [0; 3],
            calls: [None; 3],
            clock: 0,
        }
    }

    fn apply(&mut self, action: Action, rule: Rule) -> bool {
        match action {
            Action::Fork(source, target) => {
                if self.namespaces[source] != Some(Status::Active)
                    || self.namespaces[target].is_some()
                {
                    return false;
                }
                self.namespaces[target] = Some(Status::Active);
                self.inherited_owners[target] |= self.inherited_owners[source];
                self.records[target] = Some(Record { source });
            }
            Action::Issue(namespace) => {
                if self.namespaces[namespace] != Some(Status::Active) {
                    return false;
                }
                self.capabilities_until[namespace] = self.clock + CAPABILITY_LIFETIME;
            }
            Action::Delete(namespace) => {
                if self.namespaces[namespace] != Some(Status::Active) {
                    return false;
                }
                self.namespaces[namespace] = Some(Status::Deleted);
            }
            Action::Start(namespace) => {
                if self.namespaces[namespace].is_none() || self.calls[namespace].is_some() {
                    return false;
                }
                self.calls[namespace] = Some(self.clock);
            }
            Action::Gc(namespace) => {
                let Some(started) = self.calls[namespace].take() else {
                    return false;
                };
                self.gc(namespace, started, rule);
            }
            Action::Advance => self.clock += 1,
        }
        true
    }

    fn gc(&mut self, namespace: usize, started: u64, rule: Rule) {
        let retired = matches!(
            self.namespaces[namespace],
            Some(Status::Retired { deadline }) if started >= deadline
        );
        if retired
            || (matches!(rule, Rule::ReleaseUnretired)
                && matches!(
                    self.namespaces[namespace],
                    Some(Status::Deleted | Status::Retired { .. })
                ))
        {
            self.records[namespace] = None;
        }
        let retained = self
            .records
            .iter()
            .flatten()
            .any(|record| record.source == namespace);
        if self.namespaces[namespace] == Some(Status::Deleted)
            && !retained
            && (matches!(rule, Rule::Backdate) || self.clock - started <= BUDGET)
        {
            self.namespaces[namespace] = Some(Status::Retired {
                deadline: started + GRACE,
            });
        }
        self.collectable[namespace] |= retired;
    }

    fn safe(&self) -> bool {
        if self
            .records
            .iter()
            .flatten()
            .any(|record| self.collectable[record.source])
        {
            return false;
        }
        for namespace in 0..3 {
            let protected = matches!(
                self.namespaces[namespace],
                Some(Status::Active | Status::Deleted)
            ) || self.clock < self.capabilities_until[namespace];
            if !protected {
                continue;
            }
            for owner in 0..3 {
                if self.inherited_owners[namespace] & (1 << owner) != 0 && self.collectable[owner] {
                    return false;
                }
            }
        }
        true
    }

    fn converges(mut self) -> bool {
        for namespace in 0..3 {
            self.apply(Action::Gc(namespace), Rule::Correct);
        }
        for _ in 0..16 {
            self.apply(Action::Advance, Rule::Correct);
            for namespace in 0..3 {
                self.apply(Action::Start(namespace), Rule::Correct);
                self.apply(Action::Gc(namespace), Rule::Correct);
                if !self.safe() {
                    return false;
                }
            }
        }
        (0..3).all(|namespace| match self.namespaces[namespace] {
            None | Some(Status::Active) => !self.collectable[namespace],
            Some(Status::Deleted | Status::Retired { .. }) => self.collectable[namespace],
        })
    }
}

fn explore(
    model: Model,
    actions: &[Action],
    remaining: u64,
    seen: &mut HashSet<(Model, u64)>,
    completed: &mut usize,
) -> bool {
    if !model.safe() {
        return false;
    }
    if !seen.insert((model.clone(), remaining)) {
        return true;
    }
    if remaining == 0 {
        *completed += 1;
        return model.converges();
    }
    for (index, action) in actions.iter().enumerate() {
        if remaining & (1 << index) == 0 {
            continue;
        }
        let mut next = model.clone();
        if next.apply(*action, Rule::Correct)
            && !explore(next, actions, remaining & !(1 << index), seen, completed)
        {
            return false;
        }
    }
    true
}

use Action::{Advance, Delete, Fork, Gc, Issue, Start};

#[test]
fn every_bounded_interleaving_preserves_owners_and_deleted_families_converge() {
    let family = [
        Fork(0, 1),
        Fork(1, 2),
        Issue(2),
        Delete(0),
        Delete(1),
        Delete(2),
        Start(0),
        Gc(0),
        Start(1),
        Gc(1),
        Start(2),
        Gc(2),
        Advance,
        Advance,
    ];
    let leaf = [
        Fork(0, 1),
        Issue(1),
        Delete(1),
        Start(0),
        Gc(0),
        Start(1),
        Gc(1),
        Advance,
    ];
    for actions in [&family[..], &leaf[..]] {
        let mut completed = 0;
        assert!(explore(
            Model::new(),
            actions,
            (1 << actions.len()) - 1,
            &mut HashSet::new(),
            &mut completed
        ));
        assert!(completed > 0);
    }
}

#[test]
fn removing_unretired_target_retention_breaks_safety() {
    let actions = [
        Fork(0, 1),
        Fork(1, 2),
        Delete(0),
        Delete(1),
        Start(1),
        Gc(1),
        Start(0),
        Gc(0),
        Advance,
        Advance,
        Advance,
        Start(0),
        Gc(0),
    ];
    for rule in [Rule::Correct, Rule::ReleaseUnretired] {
        let mut model = Model::new();
        for action in actions {
            assert!(model.apply(action, rule));
        }
        assert_eq!(model.safe(), matches!(rule, Rule::Correct));
    }
}

#[test]
fn retirement_after_the_call_budget_can_collect_a_live_capability() {
    let actions = [
        Start(0),
        Advance,
        Advance,
        Advance,
        Issue(0),
        Delete(0),
        Gc(0),
        Start(0),
        Gc(0),
    ];
    for rule in [Rule::Correct, Rule::Backdate] {
        let mut model = Model::new();
        for action in actions {
            assert!(model.apply(action, rule));
        }
        assert!(model.clock < model.capabilities_until[0]);
        assert_eq!(model.safe(), matches!(rule, Rule::Correct));
        assert_eq!(model.collectable[0], matches!(rule, Rule::Backdate));
    }
}
