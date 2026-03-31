import Foundation
import Testing
@testable import LoonFileProviderSampleCore

struct BridgeInteropTests {
    @Test
    func bridgeClientEncodesRequestsAndDecodesSuccessPayloads() throws {
        let caller = FakeBridgeFunctionCaller()
        caller.openResponse = """
        {"ok":true,"result":{"bridge_handle":7},"error":null}
        """
        caller.listRootResponse = """
        {"ok":true,"result":{"items":[{"item_id":"root","parent_item_id":null,"display_name":"","inode_kind":"dir","materialization_state":"synthetic_dir","current_relative_path":null,"size_bytes":null,"revision_no":null,"content_digest":null,"content_manifest_digest":null}],"warnings":[]},"error":null}
        """

        let client = BridgeClient(functions: caller)
        let handle = try client.open(
            config: SampleConfig(
                opsConfigPath: "/tmp/ops.toml",
                exposedNamespaces: ["demo"],
                domainIdentifier: "main",
                domainDisplayName: "Loon",
                appGroupIdentifier: SampleDefaults.appGroupIdentifier
            )
        )
        #expect(handle == 7)
        #expect(
            try jsonObjectsEqual(
                caller.lastOpenRequestJSON,
                #"{"exposed_namespaces":["demo"],"ops_config_path":"\/tmp\/ops.toml"}"#
            )
        )

        let listing = try client.listRoot()
        #expect(listing.items.count == 1)
        #expect(listing.items[0].itemId == "root")
        #expect(
            try jsonObjectsEqual(caller.lastListRootRequestJSON, #"{"bridge_handle":7}"#)
        )
    }

    @Test
    func bridgeClientSurfacesBridgeFailures() throws {
        let caller = FakeBridgeFunctionCaller()
        caller.openResponse = """
        {"ok":false,"result":null,"error":{"code":"ops_config_load_failed","message":"bad path"}}
        """
        let client = BridgeClient(functions: caller)

        #expect(throws: BridgeInteropError.bridgeFailure(code: "ops_config_load_failed", message: "bad path")) {
            try client.open(
                config: SampleConfig(
                    opsConfigPath: "/tmp/missing.toml",
                    exposedNamespaces: ["demo"],
                    domainIdentifier: "main",
                    domainDisplayName: "Loon",
                    appGroupIdentifier: SampleDefaults.appGroupIdentifier
                )
            )
        }
    }
}

private func jsonObjectsEqual(_ lhs: String?, _ rhs: String) throws -> Bool {
    let lhsString = try #require(lhs)
    let lhsData = try #require(lhsString.data(using: .utf8))
    let rhsData = Data(rhs.utf8)
    let lhsObject = try JSONSerialization.jsonObject(with: lhsData)
    let rhsObject = try JSONSerialization.jsonObject(with: rhsData)
    return (lhsObject as AnyObject).isEqual(rhsObject)
}

private final class FakeBridgeFunctionCaller: BridgeFunctionCalling {
    var openResponse = ""
    var closeResponse = #"{"ok":true,"result":{"closed":true},"error":null}"#
    var listRootResponse = ""
    var lookupItemResponse = ""
    var listChildrenResponse = ""
    var materializeItemResponse = ""

    var lastOpenRequestJSON: String?
    var lastListRootRequestJSON: String?

    func open(requestJSON: String) throws -> String {
        lastOpenRequestJSON = requestJSON
        return openResponse
    }

    func close(requestJSON: String) throws -> String {
        closeResponse
    }

    func listRoot(requestJSON: String) throws -> String {
        lastListRootRequestJSON = requestJSON
        return listRootResponse
    }

    func lookupItem(requestJSON: String) throws -> String {
        lookupItemResponse
    }

    func listChildren(requestJSON: String) throws -> String {
        listChildrenResponse
    }

    func materializeItem(requestJSON: String) throws -> String {
        materializeItemResponse
    }
}
