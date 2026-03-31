import Foundation

public enum SampleProviderIdentifier: Equatable {
    case rootContainer
    case opaque(String)
}

public enum BridgeEnumerationTarget: Equatable {
    case root
    case children(parentItemId: String)
}

public enum SampleProviderError: Error, Equatable {
    case unavailable(String)
    case bridgeFailure(code: String, message: String)
}

public enum ExtensionAdapter {
    public static func enumerationTarget(
        for parentIdentifier: SampleProviderIdentifier
    ) -> BridgeEnumerationTarget {
        switch parentIdentifier {
        case .rootContainer:
            return .root
        case let .opaque(itemId):
            return .children(parentItemId: itemId)
        }
    }

    public static func opaqueBridgeItemId(
        for identifier: SampleProviderIdentifier
    ) -> String? {
        switch identifier {
        case .rootContainer:
            return nil
        case let .opaque(itemId):
            return itemId
        }
    }

    public static func warningLogMessages(
        for warnings: [BridgeProjectionWarning]
    ) -> [String] {
        warnings.map { warning in
            "Skipping unsupported provider item in namespace \(warning.namespaceId) at \(warning.relativePath): \(warning.reason) [\(warning.inodeKind)]"
        }
    }

    public static func mapBridgeError(_ error: BridgeInteropError) -> SampleProviderError {
        switch error {
        case let .bridgeFailure(code, message):
            if code == "targeted_materialize_failed",
               message.contains("provider_inode_unavailable")
                || message.contains("provider_inode_path_unavailable")
                || message.contains("provider_local_only_unavailable")
                || message.contains("provider_local_only_path_unavailable")
            {
                return .unavailable(message)
            }
            return .bridgeFailure(code: code, message: message)
        case let .transportFailure(message):
            return .bridgeFailure(code: "transport_failure", message: message)
        case let .invalidResponse(message):
            return .bridgeFailure(code: "invalid_response", message: message)
        case .missingBridgeHandle:
            return .bridgeFailure(
                code: "missing_bridge_handle",
                message: "Bridge handle has not been opened."
            )
        }
    }
}
