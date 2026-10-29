//! Names of the request headers that identify who a v0 HTTP request acts as.

/// The header that names the actor a mutation records.
pub const ACTOR: &str = "Loonfs-Actor";

/// The header that includes the stable subject id.
pub const SUBJECT: &str = "Loonfs-Subject";

/// The header that names the identity domain of the subject's principals.
pub const PRINCIPAL_SCOPE: &str = "Loonfs-Principal-Scope";

/// The header that includes the comma-separated principal ids.
pub const PRINCIPALS: &str = "Loonfs-Principals";
