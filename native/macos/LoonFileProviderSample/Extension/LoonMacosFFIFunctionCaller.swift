import Foundation

final class LoonMacosFFIFunctionCaller: BridgeFunctionCalling {
    typealias BridgeFunction = @convention(c) (UnsafePointer<CChar>?) -> UnsafeMutablePointer<CChar>?

    func open(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_open, requestJSON: requestJSON)
    }

    func close(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_close, requestJSON: requestJSON)
    }

    func listRoot(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_list_root, requestJSON: requestJSON)
    }

    func lookupItem(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_lookup_item, requestJSON: requestJSON)
    }

    func listChildren(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_list_children, requestJSON: requestJSON)
    }

    func materializeItem(requestJSON: String) throws -> String {
        try call(function: loon_file_provider_bridge_materialize_item, requestJSON: requestJSON)
    }

    private func call(function: BridgeFunction, requestJSON: String) throws -> String {
        let responsePointer = requestJSON.withCString { function($0) }
        guard let responsePointer else {
            throw BridgeInteropError.transportFailure("Rust bridge returned a null response pointer.")
        }
        defer { loon_file_provider_string_free(responsePointer) }

        guard let response = String(validatingUTF8: responsePointer) else {
            throw BridgeInteropError.transportFailure("Rust bridge returned invalid UTF-8.")
        }
        return response
    }
}
