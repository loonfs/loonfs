//! Executable design model, not a production collector.
//!
//! See docs/design/content-lifecycle.md for the protocol and proof premises.
//! A capture here is one complete, protected namespace cut. Object-store reads,
//! metadata traversal, clocks, and upload admission bounds need integration tests.

use std::collections::HashSet;

const NAMESPACES: usize = 3;
const OLD: u8 = 1;
const FRESH: u8 = 2;
type Roots = u8;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum Head {
    #[default]
    Absent,
    Active {
        version: u8,
        roots: Roots,
    },
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Pin {
    source: usize,
    roots: Roots,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Commit {
    namespace: usize,
    version: u8,
    roots: Roots,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    Capturing,
    Sealed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Run {
    id: u8,
    members: u8,
    captured: u8,
    roots: Roots,
    candidates: Roots,
    phase: Phase,
}

// Deliberately independent of current run ownership: a provider request already
// issued by an old worker can finish after a newer run starts.
#[derive(Clone, Copy, Debug)]
struct DeleteRequest(Roots);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rules {
    Safe,
    NoRegistrationBarrier,
    EarlyPinRelease,
    NoCreationFence,
    NoHeadCas,
    UnsealedDelete,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Model {
    objects: Roots,
    heads: [Head; NAMESPACES],
    intents: [Option<Roots>; NAMESPACES],
    pins: [Option<Pin>; NAMESPACES],
    members: u8,
    control_version: u8,
    next_run: u8,
    run: Option<Run>,
    commit: Option<Commit>,
    admitted_view: Option<Pin>,
    // Abstracts expiry of every preparation right AND admitted publication.
    // Fresh identities are never candidates until this becomes false.
    fresh_admission_open: bool,
}

impl Default for Model {
    fn default() -> Self {
        Self {
            objects: OLD | FRESH,
            heads: [
                Head::Active {
                    version: 0,
                    roots: OLD,
                },
                Head::Absent,
                Head::Absent,
            ],
            intents: [Some(OLD), None, None],
            pins: [None; NAMESPACES],
            members: 1,
            control_version: 0,
            next_run: 0,
            run: None,
            commit: None,
            admitted_view: None,
            fresh_admission_open: true,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    PrepareFork(usize, usize),
    Register(usize),
    ReleasePin(usize),
    Install(usize),
    Cancel(usize),
    Delete(usize),
    Retire(usize),
    AdmitView(usize),
    ReleaseView,
    PrepareCopy(usize),
    Expire(usize),
    PublishCopy,
    PublishFresh(usize),
    Begin,
    Capture(usize),
    Seal,
    Sweep,
    Finish,
}

impl Model {
    fn capturing(&self) -> bool {
        self.run.is_some_and(|run| run.phase == Phase::Capturing)
    }

    fn register(&mut self, target: usize, expected_version: u8, rules: Rules) -> bool {
        if expected_version != self.control_version
            || (self.capturing() && rules != Rules::NoRegistrationBarrier)
            || self.intents[target].is_none()
        {
            return false;
        }
        self.members |= 1 << target;
        self.control_version += 1;
        true
    }

    fn capture(&mut self, namespace: usize, available: bool) -> bool {
        let Some(run) = self.run else { return false };
        if run.phase != Phase::Capturing {
            return false;
        }
        let bit = 1 << namespace;
        if run.members & bit == 0 {
            return true;
        }
        if !available {
            return false;
        }
        // All still-promised views are represented by roots, not just the
        // current file revision. Pins remain relevant on a terminal source.
        let mut roots = match self.heads[namespace] {
            Head::Active { roots, .. } => roots,
            Head::Absent => self.intents[namespace].expect("registered creation intent"),
            Head::Deleted => 0,
        };
        for pin in self.pins.iter().flatten() {
            if pin.source == namespace {
                roots |= pin.roots;
            }
        }
        if let Some(view) = self.admitted_view {
            if view.source == namespace {
                roots |= view.roots;
            }
        }
        self.run = Some(Run {
            captured: run.captured | bit,
            roots: run.roots | roots,
            ..run
        });
        true
    }

    fn seal(&mut self, expected_run: u8, expected_version: u8) -> bool {
        let Some(run) = self.run else { return false };
        if run.id != expected_run
            || self.control_version != expected_version
            || run.phase != Phase::Capturing
            || run.captured != run.members
        {
            return false;
        }
        // The model collapses protected-root sealing and content-mark traversal:
        // roots are already object sets. Production must durably seal both.
        self.run = Some(Run {
            phase: Phase::Sealed,
            ..run
        });
        self.control_version += 1;
        true
    }

    fn delete_request(&self, rules: Rules) -> Option<DeleteRequest> {
        let run = self.run?;
        if run.phase != Phase::Sealed && rules != Rules::UnsealedDelete {
            return None;
        }
        Some(DeleteRequest(run.candidates & !run.roots))
    }

    fn deliver_delete(&mut self, request: DeleteRequest) {
        self.objects &= !request.0;
    }

    fn abort(&mut self, expected_run: u8) -> bool {
        if !self
            .run
            .is_some_and(|run| run.id == expected_run && run.phase == Phase::Capturing)
        {
            return false;
        }
        self.run = None;
        self.control_version += 1;
        true
    }

    // False means the actor must wait/retry. A lost create/head CAS is a
    // completed rejected operation, so it advances the actor without writing.
    fn step(&mut self, action: Action, rules: Rules) -> bool {
        match action {
            Action::PrepareFork(source, target) => {
                let Head::Active { roots, .. } = self.heads[source] else {
                    return false;
                };
                self.intents[target] = Some(roots);
                self.pins[target] = Some(Pin { source, roots });
            }
            Action::Register(target) => return self.register(target, self.control_version, rules),
            Action::ReleasePin(target) => {
                if self.members & (1 << target) == 0
                    && self.heads[target] != Head::Deleted
                    && rules != Rules::EarlyPinRelease
                {
                    return false;
                }
                self.pins[target] = None;
            }
            Action::Install(target) => {
                if self.members & (1 << target) == 0 {
                    return false;
                }
                if self.heads[target] == Head::Absent {
                    self.heads[target] = Head::Active {
                        version: 0,
                        roots: self.intents[target].expect("registered intent"),
                    };
                }
            }
            Action::Cancel(target) => {
                if self.heads[target] == Head::Absent {
                    if rules != Rules::NoCreationFence {
                        self.heads[target] = Head::Deleted;
                    }
                    // Drop protection only after winning the terminal-head CAS.
                    self.pins[target] = None;
                }
            }
            Action::Delete(namespace) => self.heads[namespace] = Head::Deleted,
            Action::Retire(namespace) => {
                if self.capturing()
                    || self.heads[namespace] != Head::Deleted
                    || self
                        .admitted_view
                        .is_some_and(|view| view.source == namespace)
                    || self
                        .pins
                        .iter()
                        .flatten()
                        .any(|pin| pin.source == namespace)
                {
                    return false;
                }
                self.members &= !(1 << namespace);
                self.control_version += 1;
            }
            Action::AdmitView(namespace) => {
                let Head::Active { roots, .. } = self.heads[namespace] else {
                    return false;
                };
                self.admitted_view = Some(Pin {
                    source: namespace,
                    roots,
                });
            }
            Action::ReleaseView => self.admitted_view = None,
            Action::PrepareCopy(namespace) => {
                let Head::Active { version, roots } = self.heads[namespace] else {
                    return false;
                };
                self.commit = Some(Commit {
                    namespace,
                    version,
                    roots,
                });
            }
            Action::Expire(namespace) => {
                let Head::Active { version, .. } = self.heads[namespace] else {
                    return false;
                };
                // Abstract explicit release of the final local promise, after
                // the namespace has released it. Independent admitted views
                // retain their own protection.
                self.heads[namespace] = Head::Active {
                    version: version + 1,
                    roots: 0,
                };
            }
            Action::PublishCopy => {
                let commit = self.commit.take().expect("prepared copy");
                if let Head::Active { version, .. } = self.heads[commit.namespace] {
                    if version == commit.version || rules == Rules::NoHeadCas {
                        self.heads[commit.namespace] = Head::Active {
                            version: version + 1,
                            roots: commit.roots,
                        };
                    }
                }
            }
            Action::PublishFresh(namespace) => {
                let Head::Active { version, roots } = self.heads[namespace] else {
                    return false;
                };
                if self.fresh_admission_open {
                    self.heads[namespace] = Head::Active {
                        version: version + 1,
                        roots: roots | FRESH,
                    };
                }
            }
            Action::Begin => {
                if self.run.is_some() {
                    return false;
                }
                self.next_run += 1;
                self.control_version += 1;
                self.run = Some(Run {
                    id: self.next_run,
                    members: self.members,
                    captured: 0,
                    roots: 0,
                    candidates: if self.fresh_admission_open {
                        OLD
                    } else {
                        OLD | FRESH
                    },
                    phase: Phase::Capturing,
                });
            }
            Action::Capture(namespace) => return self.capture(namespace, true),
            Action::Seal => {
                let run = self.run.expect("started run");
                return self.seal(run.id, self.control_version);
            }
            Action::Sweep => {
                let Some(request) = self.delete_request(rules) else {
                    return false;
                };
                self.deliver_delete(request);
            }
            Action::Finish => {
                if !self.run.is_some_and(|run| run.phase == Phase::Sealed) {
                    return false;
                }
                self.run = None;
                self.control_version += 1;
            }
        }
        true
    }

    fn safe(&self) -> bool {
        let mut promised = 0;
        for (namespace, head) in self.heads.iter().enumerate() {
            match head {
                Head::Active { roots, .. } => {
                    if self.members & (1 << namespace) == 0 {
                        return false;
                    }
                    promised |= roots;
                }
                Head::Absent if self.members & (1 << namespace) != 0 => {
                    promised |= self.intents[namespace].expect("registered intent");
                }
                _ => {}
            }
        }
        for pin in self.pins.iter().flatten() {
            promised |= pin.roots;
        }
        if let Some(view) = self.admitted_view {
            promised |= view.roots;
        }
        promised & !self.objects == 0
    }
}

const COLLECT: &[Action] = &[
    Action::Begin,
    Action::Capture(0),
    Action::Capture(1),
    Action::Capture(2),
    Action::Seal,
    Action::Sweep,
    Action::Finish,
];

#[derive(Default)]
struct Exploration {
    states: usize,
    completed: usize,
    counterexample: Option<Vec<Action>>,
}

fn explore(initial: Model, actors: &[&[Action]], rules: Rules) -> Exploration {
    let mut result = Exploration::default();
    let mut seen = HashSet::new();
    let mut pending = vec![(initial, vec![0; actors.len()], Vec::new())];
    while let Some((model, positions, trace)) = pending.pop() {
        if !seen.insert((model.clone(), positions.clone())) {
            continue;
        }
        result.states += 1;
        if !model.safe() {
            result.counterexample = Some(trace);
            return result;
        }
        if positions
            .iter()
            .zip(actors)
            .all(|(position, actor)| *position == actor.len())
        {
            result.completed += 1;
        }
        for (index, actor) in actors.iter().enumerate() {
            let Some(&action) = actor.get(positions[index]) else {
                continue;
            };
            let mut next = model.clone();
            if next.step(action, rules) {
                let mut next_positions = positions.clone();
                next_positions[index] += 1;
                let mut next_trace = trace.clone();
                next_trace.push(action);
                pending.push((next, next_positions, next_trace));
            }
        }
    }
    result
}

fn assert_safe_schedules(initial: Model, actors: &[&[Action]]) {
    let result = explore(initial, actors, Rules::Safe);
    assert!(
        result.counterexample.is_none(),
        "unsafe trace: {:?}",
        result.counterexample
    );
    assert!(
        result.completed > 0,
        "no complete schedule in {} states",
        result.states
    );
    assert!(result.states > 10, "scenario explored too little state");
}

fn apply(model: &mut Model, actions: &[Action]) {
    for &action in actions {
        assert!(model.step(action, Rules::Safe), "blocked step: {action:?}");
        assert!(model.safe(), "unsafe after {action:?}");
    }
}

#[test]
fn fork_registration_and_source_deletion_all_schedules() {
    let fork = &[
        Action::PrepareFork(0, 1),
        Action::Register(1),
        Action::ReleasePin(1),
        Action::Install(1),
    ];
    assert_safe_schedules(Model::default(), &[fork, &[Action::Delete(0)], COLLECT]);
    let broken = explore(
        Model::default(),
        &[fork, &[Action::Delete(0)], COLLECT],
        Rules::NoRegistrationBarrier,
    );
    assert!(
        broken.counterexample.is_some(),
        "barrier removal must expose a lost fork"
    );
}

#[test]
fn source_protection_cannot_end_before_registration() {
    let fork = &[
        Action::PrepareFork(0, 1),
        Action::ReleasePin(1),
        Action::Register(1),
        Action::Install(1),
    ];
    // This actor order must block under the protocol. Removing the ordering
    // requirement lets collection miss both source and target.
    let result = explore(
        Model::default(),
        &[fork, &[Action::Delete(0)], COLLECT],
        Rules::Safe,
    );
    assert!(result.counterexample.is_none());
    assert_eq!(result.completed, 0);
    let broken = explore(
        Model::default(),
        &[fork, &[Action::Delete(0)], COLLECT],
        Rules::EarlyPinRelease,
    );
    assert!(broken.counterexample.is_some());
}

#[test]
fn cancelled_creator_cannot_resume_into_collected_content() {
    let mut initial = Model::default();
    apply(&mut initial, &[Action::PrepareFork(0, 1)]);
    let creator = &[Action::Register(1), Action::Install(1)];
    let cleanup = &[Action::Cancel(1), Action::Delete(0)];
    assert_safe_schedules(initial.clone(), &[creator, cleanup, COLLECT]);
    let broken = explore(
        initial,
        &[creator, cleanup, COLLECT],
        Rules::NoCreationFence,
    );
    assert!(
        broken.counterexample.is_some(),
        "timeout-only cleanup must expose a late creator"
    );
}

#[test]
fn registered_creation_is_a_root_before_head_installation() {
    let mut model = Model::default();
    apply(
        &mut model,
        &[
            Action::PrepareFork(0, 1),
            Action::Register(1),
            Action::ReleasePin(1),
            Action::Delete(0),
        ],
    );
    apply(&mut model, COLLECT);
    assert_ne!(model.objects & OLD, 0);
    apply(&mut model, &[Action::Install(1)]);
}

#[test]
fn nested_fork_survives_deleted_ancestors_and_eventually_reclaims() {
    let mut initial = Model::default();
    apply(
        &mut initial,
        &[
            Action::PrepareFork(0, 1),
            Action::Register(1),
            Action::ReleasePin(1),
            Action::Install(1),
        ],
    );
    let grandchild = &[
        Action::PrepareFork(1, 2),
        Action::Register(2),
        Action::ReleasePin(2),
        Action::Install(2),
    ];
    assert_safe_schedules(
        initial.clone(),
        &[grandchild, &[Action::Delete(0), Action::Delete(1)], COLLECT],
    );
    apply(&mut initial, grandchild);
    apply(
        &mut initial,
        &[
            Action::Delete(0),
            Action::Delete(1),
            Action::Retire(0),
            Action::Retire(1),
        ],
    );
    apply(&mut initial, COLLECT);
    assert_ne!(initial.objects & OLD, 0);
    apply(&mut initial, &[Action::Delete(2), Action::Retire(2)]);
    apply(&mut initial, COLLECT);
    assert_eq!(initial.objects & OLD, 0);
    assert_eq!(initial.members, 0);
}

#[test]
fn prepared_local_copy_cannot_restore_a_released_promise() {
    let actors: &[&[Action]] = &[
        &[Action::PrepareCopy(0), Action::PublishCopy],
        &[Action::Expire(0)],
        COLLECT,
    ];
    assert_safe_schedules(Model::default(), actors);
    let broken = explore(Model::default(), actors, Rules::NoHeadCas);
    assert!(broken.counterexample.is_some());
}

#[test]
fn fresh_publication_is_excluded_until_all_admission_has_ended() {
    assert_safe_schedules(Model::default(), &[&[Action::PublishFresh(0)], COLLECT]);
    let mut model = Model {
        fresh_admission_open: false,
        ..Model::default()
    };
    apply(&mut model, COLLECT);
    assert_eq!(model.objects & FRESH, 0);
    apply(&mut model, &[Action::PublishFresh(0)]);
    assert!(
        model.safe(),
        "an expired proof must not resurrect the reclaimed identity"
    );
}

#[test]
fn unavailable_member_and_unsealed_marks_never_authorize_delete() {
    let mut model = Model::default();
    apply(&mut model, &[Action::Begin]);
    assert!(!model.capture(0, false));
    assert!(!model.step(Action::Seal, Rules::Safe));
    assert!(model.delete_request(Rules::Safe).is_none());
    let request = model
        .delete_request(Rules::UnsealedDelete)
        .expect("negative control");
    let mut broken = model.clone();
    broken.deliver_delete(request);
    assert!(
        !broken.safe(),
        "partial marks must expose the missing live root"
    );
    // A different worker can resume the same durable model state after failure.
    let mut resumed = model.clone();
    apply(&mut resumed, &COLLECT[1..]);
    assert!(resumed.safe());
}

#[test]
fn stale_registration_and_aborted_capture_cannot_bypass_control_cas() {
    let mut model = Model::default();
    apply(&mut model, &[Action::PrepareFork(0, 1)]);
    let observed_version = model.control_version;
    apply(&mut model, &[Action::Begin]);
    assert!(!model.register(1, observed_version, Rules::Safe));
    let old_run = model.run.expect("run").id;
    let old_version = model.control_version;
    assert!(model.abort(old_run));
    apply(&mut model, &[Action::Register(1), Action::Begin]);
    assert!(!model.seal(old_run, old_version));
    assert!(!model.abort(old_run));
    assert!(model.delete_request(Rules::Safe).is_none());
    apply(&mut model, &COLLECT[1..]);
}

#[test]
fn delayed_delete_from_an_old_run_remains_safe_in_a_new_run() {
    let mut model = Model::default();
    apply(&mut model, &[Action::Expire(0)]);
    apply(&mut model, &COLLECT[..5]);
    let delayed = model.delete_request(Rules::Safe).expect("sealed request");
    apply(&mut model, &[Action::Finish, Action::PublishFresh(0)]);
    apply(
        &mut model,
        &[
            Action::PrepareFork(0, 1),
            Action::Register(1),
            Action::ReleasePin(1),
            Action::Install(1),
            Action::Begin,
        ],
    );
    model.deliver_delete(delayed);
    assert!(model.safe());
    assert_eq!(model.objects, FRESH);
    apply(&mut model, &COLLECT[1..]);
}

#[test]
fn retirement_waits_for_capture_and_outgoing_transfer() {
    let mut model = Model::default();
    apply(&mut model, &[Action::PrepareFork(0, 1), Action::Delete(0)]);
    assert!(!model.step(Action::Retire(0), Rules::Safe));
    apply(
        &mut model,
        &[Action::Register(1), Action::ReleasePin(1), Action::Begin],
    );
    assert!(!model.step(Action::Retire(0), Rules::Safe));
    apply(&mut model, &COLLECT[1..]);
    apply(&mut model, &[Action::Retire(0), Action::Install(1)]);
}

#[test]
fn admitted_view_outlives_release_of_its_namespace_promise() {
    let actors: &[&[Action]] = &[
        &[Action::AdmitView(0), Action::ReleaseView],
        &[Action::Expire(0), Action::Delete(0)],
        COLLECT,
    ];
    assert_safe_schedules(Model::default(), actors);
    let mut model = Model::default();
    apply(&mut model, &[Action::AdmitView(0), Action::Delete(0)]);
    assert!(!model.step(Action::Retire(0), Rules::Safe));
    apply(&mut model, COLLECT);
    assert_ne!(model.objects & OLD, 0);
    apply(&mut model, &[Action::ReleaseView, Action::Retire(0)]);
    apply(&mut model, COLLECT);
    assert_eq!(model.objects & OLD, 0);
}

#[test]
fn cancelling_an_absent_target_and_retiring_it_eventually_reclaims() {
    let mut model = Model::default();
    apply(
        &mut model,
        &[
            Action::PrepareFork(0, 1),
            Action::Register(1),
            Action::Cancel(1),
            Action::Delete(0),
            Action::Retire(0),
            Action::Retire(1),
        ],
    );
    apply(&mut model, COLLECT);
    assert_eq!(model.objects & OLD, 0);
    assert_eq!(model.members, 0);
    // A registration CAS delayed until after cleanup may leave an extra row;
    // its delayed head installation still loses to the terminal head.
    apply(
        &mut model,
        &[Action::Register(1), Action::Install(1), Action::Retire(1)],
    );
    assert_eq!(model.heads[1], Head::Deleted);
}
