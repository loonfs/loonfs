import Foundation

public enum BridgeMaterializationState: String, Codable, Equatable, Sendable {
    case syntheticDir = "synthetic_dir"
    case materialized
    case placeholder
    case unavailable
}

public struct BridgeProjectionWarning: Codable, Equatable, Sendable {
    public let namespaceId: String
    public let relativePath: String
    public let inodeKind: String
    public let reason: String
}

public struct BridgeItemSnapshot: Codable, Equatable, Sendable {
    public let itemId: String
    public let parentItemId: String?
    public let displayName: String
    public let inodeKind: String
    public let materializationState: BridgeMaterializationState
    public let currentRelativePath: String?
    public let sizeBytes: UInt64?
    public let revisionNo: UInt64?
    public let contentDigest: String?
    public let contentManifestDigest: String?
}

public struct BridgeListing: Codable, Equatable, Sendable {
    public let items: [BridgeItemSnapshot]
    public let warnings: [BridgeProjectionWarning]
}

public struct BridgeLookupResult: Codable, Equatable, Sendable {
    public let item: BridgeItemSnapshot?
    public let warnings: [BridgeProjectionWarning]
}

public struct BridgeMaterializedPath: Codable, Equatable, Sendable {
    public let absolutePath: String
    public let relativePath: String
}

public enum BridgeInteropError: Error, Equatable, CustomStringConvertible, Sendable {
    case missingBridgeHandle
    case transportFailure(String)
    case invalidResponse(String)
    case bridgeFailure(code: String, message: String)

    public var description: String {
        switch self {
        case .missingBridgeHandle:
            return "Bridge handle has not been opened yet."
        case let .transportFailure(message):
            return "Bridge transport failed: \(message)"
        case let .invalidResponse(message):
            return "Bridge response was invalid: \(message)"
        case let .bridgeFailure(code, message):
            return "Bridge returned \(code): \(message)"
        }
    }
}

public protocol BridgeFunctionCalling {
    func open(requestJSON: String) throws -> String
    func close(requestJSON: String) throws -> String
    func listRoot(requestJSON: String) throws -> String
    func lookupItem(requestJSON: String) throws -> String
    func listChildren(requestJSON: String) throws -> String
    func materializeItem(requestJSON: String) throws -> String
}

public final class BridgeClient {
    private let functions: BridgeFunctionCalling
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder
    private var bridgeHandle: UInt64?

    public init(functions: BridgeFunctionCalling) {
        self.functions = functions
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
        self.encoder.keyEncodingStrategy = .convertToSnakeCase
        self.decoder.keyDecodingStrategy = .convertFromSnakeCase
    }

    @discardableResult
    public func open(config: SampleConfig) throws -> UInt64 {
        if let bridgeHandle {
            return bridgeHandle
        }

        let request = OpenBridgeRequest(
            opsConfigPath: config.opsConfigPath,
            exposedNamespaces: config.exposedNamespaces
        )
        let result: OpenBridgeResult = try call(
            request,
            using: functions.open
        )
        bridgeHandle = result.bridgeHandle
        return result.bridgeHandle
    }

    @discardableResult
    public func close() throws -> Bool {
        guard let bridgeHandle else {
            return false
        }

        let result: CloseBridgeResult = try call(
            BridgeHandleRequest(bridgeHandle: bridgeHandle),
            using: functions.close
        )
        if result.closed {
            self.bridgeHandle = nil
        }
        return result.closed
    }

    public func listRoot() throws -> BridgeListing {
        try callWithBridgeHandle(using: functions.listRoot)
    }

    public func lookupItem(itemId: String) throws -> BridgeLookupResult {
        try call(
            LookupItemRequest(
                bridgeHandle: try requireBridgeHandle(),
                itemId: itemId
            ),
            using: functions.lookupItem
        )
    }

    public func listChildren(parentItemId: String) throws -> BridgeListing {
        try call(
            ListChildrenRequest(
                bridgeHandle: try requireBridgeHandle(),
                parentItemId: parentItemId
            ),
            using: functions.listChildren
        )
    }

    public func materializeItem(itemId: String, nowMs: UInt64) throws -> BridgeMaterializedPath {
        try call(
            MaterializeItemRequest(
                bridgeHandle: try requireBridgeHandle(),
                itemId: itemId,
                nowMs: nowMs
            ),
            using: functions.materializeItem
        )
    }

    private func requireBridgeHandle() throws -> UInt64 {
        guard let bridgeHandle else {
            throw BridgeInteropError.missingBridgeHandle
        }
        return bridgeHandle
    }

    private func callWithBridgeHandle<Response: Decodable>(
        using function: (String) throws -> String
    ) throws -> Response {
        let request = BridgeHandleRequest(bridgeHandle: try requireBridgeHandle())
        return try call(request, using: function)
    }

    private func call<Request: Encodable, Response: Decodable>(
        _ request: Request,
        using function: (String) throws -> String
    ) throws -> Response {
        let requestJSON: String
        do {
            requestJSON = String(decoding: try encoder.encode(request), as: UTF8.self)
        } catch {
            throw BridgeInteropError.transportFailure(
                "Could not encode bridge request: \(error.localizedDescription)"
            )
        }

        let responseJSON = try function(requestJSON)
        let data = Data(responseJSON.utf8)
        let envelope: BridgeEnvelope<Response>
        do {
            envelope = try decoder.decode(BridgeEnvelope<Response>.self, from: data)
        } catch {
            throw BridgeInteropError.invalidResponse(error.localizedDescription)
        }

        if envelope.ok {
            guard let result = envelope.result else {
                throw BridgeInteropError.invalidResponse("Missing `result` in successful envelope.")
            }
            return result
        }

        guard let error = envelope.error else {
            throw BridgeInteropError.invalidResponse("Missing `error` in failed envelope.")
        }
        throw BridgeInteropError.bridgeFailure(code: error.code, message: error.message)
    }
}

private struct BridgeEnvelope<Result: Decodable>: Decodable {
    let ok: Bool
    let result: Result?
    let error: BridgeErrorPayload?
}

private struct BridgeErrorPayload: Codable, Equatable {
    let code: String
    let message: String
}

private struct OpenBridgeRequest: Codable {
    let opsConfigPath: String
    let exposedNamespaces: [String]
}

private struct BridgeHandleRequest: Codable {
    let bridgeHandle: UInt64
}

private struct LookupItemRequest: Codable {
    let bridgeHandle: UInt64
    let itemId: String
}

private struct ListChildrenRequest: Codable {
    let bridgeHandle: UInt64
    let parentItemId: String
}

private struct MaterializeItemRequest: Codable {
    let bridgeHandle: UInt64
    let itemId: String
    let nowMs: UInt64
}

private struct OpenBridgeResult: Codable {
    let bridgeHandle: UInt64
}

private struct CloseBridgeResult: Codable {
    let closed: Bool
}
