//! Failures while post-processing the generated OpenAPI document.

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenapiPostprocessError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("openapi operation `{operation_id}` has no retry classification")]
    MissingRetryClassification { operation_id: String },
    #[error("openapi operation `{operation_id}` has no SDK name")]
    MissingSdkName { operation_id: String },
    #[error("openapi SDK-named operation `{operation_id}` does not appear in the document")]
    MissingSdkNameOperation { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no `cursor` query parameter")]
    MissingPaginationCursorParameter { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no 200 response component schema")]
    MissingPaginationResponseSchema { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` response has no `{property}` property")]
    MissingPaginationResponseProperty {
        operation_id: String,
        property: String,
    },
    #[error(
        "openapi pagination operation `{operation_id}` response: expected exactly one array property"
    )]
    InvalidPaginationArrayProperties { operation_id: String },
    #[error(
        "openapi operation `{operation_id}` has a `cursor` query parameter but no pagination metadata entry"
    )]
    MissingPaginationMetadata { operation_id: String },
    #[error("invalid openapi document at `{location}`")]
    InvalidDocument { location: String },
}
