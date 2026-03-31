import FileProvider
import Foundation
import UniformTypeIdentifiers

final class SampleFileProviderItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let filename: String
    let contentType: UTType
    let capabilities: NSFileProviderItemCapabilities
    let documentSize: NSNumber?
    let itemVersion: NSFileProviderItemVersion
    let isUploaded = true
    let isUploading = false

    init(snapshot: BridgeItemSnapshot) {
        self.itemIdentifier = Self.fileProviderIdentifier(for: snapshot.itemId)
        self.parentItemIdentifier = Self.fileProviderIdentifier(for: snapshot.parentItemId)
        self.filename = snapshot.displayName
        self.contentType = snapshot.inodeKind == "dir" ? .folder : .data
        self.capabilities = [.allowsReading]
        self.documentSize = snapshot.sizeBytes.map(NSNumber.init(value:))
        self.itemVersion = Self.makeVersion(snapshot: snapshot)
        super.init()
    }

    init(rootDisplayName: String) {
        self.itemIdentifier = .rootContainer
        self.parentItemIdentifier = .rootContainer
        self.filename = rootDisplayName
        self.contentType = .folder
        self.capabilities = [.allowsReading]
        self.documentSize = nil
        self.itemVersion = NSFileProviderItemVersion(
            contentVersion: Data("root".utf8),
            metadataVersion: Data(rootDisplayName.utf8.prefix(128))
        )
        super.init()
    }

    private static func fileProviderIdentifier(for bridgeItemId: String?) -> NSFileProviderItemIdentifier {
        guard let bridgeItemId, bridgeItemId != "root" else {
            return .rootContainer
        }
        return NSFileProviderItemIdentifier(bridgeItemId)
    }

    private static func makeVersion(snapshot: BridgeItemSnapshot) -> NSFileProviderItemVersion {
        let contentComponent = snapshot.contentDigest
            ?? snapshot.contentManifestDigest
            ?? snapshot.itemId
        let metadataComponent = [
            snapshot.displayName,
            snapshot.currentRelativePath ?? "",
            snapshot.revisionNo.map(String.init) ?? "",
        ]
        .joined(separator: "|")
        return NSFileProviderItemVersion(
            contentVersion: Data(contentComponent.utf8.prefix(128)),
            metadataVersion: Data(metadataComponent.utf8.prefix(128))
        )
    }
}
