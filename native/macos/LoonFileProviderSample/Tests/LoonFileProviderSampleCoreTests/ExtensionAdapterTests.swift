import Testing
@testable import LoonFileProviderSampleCore

struct ExtensionAdapterTests {
    @Test
    func rootContainerMapsToBridgeRootEnumeration() {
        #expect(
            ExtensionAdapter.enumerationTarget(for: .rootContainer) == .root
        )
    }

    @Test
    func opaqueIdentifierPassesThrough() {
        #expect(
            ExtensionAdapter.opaqueBridgeItemId(for: .opaque("inode:6e73:0001"))
                == "inode:6e73:0001"
        )
    }

    @Test
    func warningMessagesIncludeNamespacePathAndKind() {
        let messages = ExtensionAdapter.warningLogMessages(
            for: [
                BridgeProjectionWarning(
                    namespaceId: "demo",
                    relativePath: "alias",
                    inodeKind: "symlink",
                    reason: "unsupported_inode_kind"
                )
            ]
        )
        #expect(
            messages
                == ["Skipping unsupported provider item in namespace demo at alias: unsupported_inode_kind [symlink]"]
        )
    }

    @Test
    func targetedMaterializeUnavailableMapsToSampleUnavailableError() {
        let error = ExtensionAdapter.mapBridgeError(
            .bridgeFailure(
                code: "targeted_materialize_failed",
                message: "provider_inode_unavailable: namespace `demo` inode `7` decision `download_remote_edit` reason `remote_path_target_parent_unusable`"
            )
        )
        #expect(
            error
                == .unavailable(
                    "provider_inode_unavailable: namespace `demo` inode `7` decision `download_remote_edit` reason `remote_path_target_parent_unusable`"
                )
        )
    }
}
