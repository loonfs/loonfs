//! Names of the request headers that identify who a v0 HTTP request acts as.

/// The header that identifies the actor: the identity of a request, which
/// LoonFS records on a commit and on an upload session.
pub const ACTOR: &str = "Loonfs-Actor";

/// The header that includes the comma-separated principal ids.
pub const PRINCIPALS: &str = "Loonfs-Principals";

/// The header that puts a request in the service context instead of acting
/// for principals.
pub const CONTEXT: &str = "Loonfs-Context";

/// The one value of [`CONTEXT`]: the service context, in which LoonFS checks
/// no rights.
pub const SERVICE_CONTEXT: &str = "service";
