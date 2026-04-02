#[path = "integration/bound_directory_child_planner.rs"]
mod bound_directory_child_planner;
#[path = "integration/checkpoint_builder_differential.rs"]
mod checkpoint_builder_differential;
#[path = "integration/checkpoint_publish_differential.rs"]
mod checkpoint_publish_differential;
#[path = "integration/client_conflict_artifact_restore.rs"]
mod client_conflict_artifact_restore;
#[path = "integration/client_conflict_resolution.rs"]
mod client_conflict_resolution;
#[path = "integration/client_crash_restart_hardening.rs"]
mod client_crash_restart_hardening;
#[path = "integration/client_create_remote_dir.rs"]
mod client_create_remote_dir;
#[path = "integration/client_dispatch_from_state.rs"]
mod client_dispatch_from_state;
#[path = "integration/client_download_remote_edit.rs"]
mod client_download_remote_edit;
#[path = "integration/client_execute_local_only_create.rs"]
mod client_execute_local_only_create;
#[path = "integration/client_execute_next_client_action.rs"]
mod client_execute_next_client_action;
#[path = "integration/client_execute_next_local_only_create.rs"]
mod client_execute_next_local_only_create;
#[path = "integration/client_mutation_executor.rs"]
mod client_mutation_executor;
#[path = "integration/client_mutation_request_from_state.rs"]
mod client_mutation_request_from_state;
#[path = "integration/client_mutation_round_trip.rs"]
mod client_mutation_round_trip;
#[path = "integration/client_observe_delete_move.rs"]
mod client_observe_delete_move;
#[path = "integration/client_observe_subtree.rs"]
mod client_observe_subtree;
#[path = "integration/client_remote_observation.rs"]
mod client_remote_observation;
#[path = "integration/client_remote_only_directory_materialization.rs"]
mod client_remote_only_directory_materialization;
#[path = "integration/client_remote_only_discovery.rs"]
mod client_remote_only_discovery;
#[path = "integration/client_remote_only_materialization.rs"]
mod client_remote_only_materialization;
#[path = "integration/client_server_explore.rs"]
mod client_server_explore;
#[path = "integration/client_server_sim.rs"]
mod client_server_sim;
#[path = "integration/client_subtree_conflict_resolution.rs"]
mod client_subtree_conflict_resolution;
#[path = "integration/client_upload_local_create.rs"]
mod client_upload_local_create;
#[path = "integration/client_upload_local_edit.rs"]
mod client_upload_local_edit;
#[path = "integration/commit_metadata_differential.rs"]
mod commit_metadata_differential;
#[path = "integration/common/mod.rs"]
mod common;
#[path = "integration/content_differential.rs"]
mod content_differential;
#[path = "integration/hot_file_planner.rs"]
mod hot_file_planner;
#[path = "integration/local_fs_events.rs"]
mod local_fs_events;
#[path = "integration/local_only_binding.rs"]
mod local_only_binding;
#[path = "integration/local_only_directory_binding.rs"]
mod local_only_directory_binding;
#[path = "integration/local_only_directory_planner.rs"]
mod local_only_directory_planner;
#[path = "integration/local_only_planner.rs"]
mod local_only_planner;
#[path = "integration/ops_file.rs"]
mod ops_file;
#[path = "integration/progress_differential.rs"]
mod progress_differential;
#[path = "integration/provider.rs"]
mod provider;
#[path = "integration/replay_differential.rs"]
mod replay_differential;
#[path = "integration/small_file_upload.rs"]
mod small_file_upload;
#[path = "integration/stale_writer_differential.rs"]
mod stale_writer_differential;
