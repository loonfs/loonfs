//! Independent exploration of namespace retirement and fork record release.

use std::collections::HashSet;

const GRACE: u64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Status {
    Active,
    Deleted,
    Retired { deadline: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RecordStatus {
    Active,
    Released { at: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Record {
    source: usize,
    status: RecordStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Run {
    started: u64,
    deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Model {
    namespaces: [Option<Status>; 3],
    records: [Option<Record>; 3],
    collectable: [bool; 3],
    inherited_owners: [u8; 3],
    retired_at: [Option<u64>; 3],
    runs: [Option<Run>; 3],
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
    Delete(usize),
    Gc(usize),
    Start(usize),
    Resume(usize),
    Advance,
}

impl Model {
    fn new() -> Self {
        Self {
            namespaces: [Some(Status::Active), None, None],
            records: [None; 3],
            collectable: [false; 3],
            inherited_owners: [1, 2, 4],
            retired_at: [None; 3],
            runs: [None; 3],
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
                self.records[target] = Some(Record {
                    source,
                    status: RecordStatus::Active,
                });
            }
            Action::Delete(namespace) => {
                if self.namespaces[namespace] != Some(Status::Active) {
                    return false;
                }
                self.namespaces[namespace] = Some(Status::Deleted);
            }
            Action::Start(namespace) => {
                if self.namespaces[namespace].is_none() || self.runs[namespace].is_some() {
                    return false;
                }
                self.runs[namespace] = Some(Run {
                    started: self.clock,
                    deleted: self.namespaces[namespace] == Some(Status::Deleted),
                });
            }
            Action::Resume(namespace) => {
                let Some(run) = self.runs[namespace].take() else {
                    return false;
                };
                self.gc(namespace, run, rule);
            }
            Action::Gc(namespace) => {
                if !self.apply(Action::Start(namespace), rule) {
                    return false;
                }
                self.apply(Action::Resume(namespace), rule);
            }
            Action::Advance => self.clock += GRACE + 1,
        }
        true
    }

    fn gc(&mut self, namespace: usize, run: Run, rule: Rule) {
        for (target, slot) in self.records.iter_mut().enumerate() {
            let Some(record) = slot else { continue };
            if record.source != namespace {
                continue;
            }
            match record.status {
                RecordStatus::Released { at } if run.started >= at + GRACE => *slot = None,
                RecordStatus::Released { .. } => {}
                RecordStatus::Active => {
                    let release = match self.namespaces[target] {
                        Some(Status::Active) | None => false,
                        Some(Status::Deleted) => matches!(rule, Rule::ReleaseUnretired),
                        Some(Status::Retired { deadline }) => deadline <= run.started,
                    };
                    if release {
                        record.status = RecordStatus::Released { at: run.started };
                    }
                }
            }
        }
        let retained = self
            .records
            .iter()
            .flatten()
            .any(|record| record.source == namespace);
        if run.deleted && self.namespaces[namespace] == Some(Status::Deleted) && !retained {
            let now = if matches!(rule, Rule::Backdate) {
                run.started
            } else {
                self.clock
            };
            self.namespaces[namespace] = Some(Status::Retired {
                deadline: now + GRACE,
            });
            self.retired_at[namespace] = Some(self.clock);
        }
        if let Some(Status::Retired { deadline }) = self.namespaces[namespace] {
            self.collectable[namespace] |= run.started >= deadline;
        }
    }

    fn safe(&self) -> bool {
        for namespace in 0..3 {
            let protected = match self.namespaces[namespace] {
                None => false,
                Some(Status::Active | Status::Deleted) => true,
                Some(Status::Retired { deadline }) => {
                    let retired_at =
                        self.retired_at[namespace].expect("retired namespace has an instant");
                    if deadline < retired_at + GRACE {
                        return false;
                    }
                    self.clock < deadline
                }
            };
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
            self.apply(Action::Resume(namespace), Rule::Correct);
        }
        for _ in 0..16 {
            self.apply(Action::Advance, Rule::Correct);
            for namespace in 0..3 {
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

use Action::{Advance, Delete, Fork, Gc, Resume, Start};

#[test]
fn every_bounded_interleaving_preserves_owners_and_deleted_families_converge() {
    let family = [
        Fork(0, 1),
        Fork(1, 2),
        Delete(0),
        Delete(1),
        Delete(2),
        Gc(0),
        Gc(1),
        Start(2),
        Resume(2),
        Advance,
        Advance,
    ];
    let leaf = [Fork(0, 1), Delete(1), Gc(0), Start(1), Resume(1), Advance];
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
    let mut model = Model::new();
    let actions = [
        Fork(0, 1),
        Fork(1, 2),
        Delete(0),
        Delete(1),
        Gc(0),
        Advance,
        Gc(0),
        Advance,
        Gc(0),
    ];
    for action in actions {
        assert!(model.apply(action, Rule::ReleaseUnretired));
    }
    assert!(!model.safe());
}

#[test]
fn using_the_run_start_for_retirement_breaks_the_grace_contract() {
    let actions = [Delete(0), Start(0), Advance, Resume(0), Gc(0)];
    for rule in [Rule::Correct, Rule::Backdate] {
        let mut model = Model::new();
        for action in actions {
            assert!(model.apply(action, rule));
        }
        assert_eq!(model.safe(), matches!(rule, Rule::Correct));
        assert_eq!(model.collectable[0], matches!(rule, Rule::Backdate));
    }
}
