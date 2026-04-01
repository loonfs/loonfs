import FileProvider
import Foundation
import OSLog

private struct SendableObserverBox<T>: @unchecked Sendable {
    let value: T
}

final class SampleFileProviderEnumerator: NSObject, NSFileProviderEnumerator {
    private let parentIdentifier: NSFileProviderItemIdentifier
    private let bridgeSession: SampleBridgeSession
    private let logger: Logger
    private let domainDisplayName: String

    init(
        parentIdentifier: NSFileProviderItemIdentifier,
        bridgeSession: SampleBridgeSession,
        logger: Logger,
        domainDisplayName: String
    ) {
        self.parentIdentifier = parentIdentifier
        self.bridgeSession = bridgeSession
        self.logger = logger
        self.domainDisplayName = domainDisplayName
    }

    func invalidate() {}

    func enumerateItems(
        for observer: NSFileProviderEnumerationObserver,
        startingAt page: NSFileProviderPage
    ) {
        let parentIdentifier = self.parentIdentifier
        let bridgeSession = self.bridgeSession
        let logger = self.logger
        let domainDisplayName = self.domainDisplayName
        let observer = SendableObserverBox(value: observer)
        Task {
            do {
                let listing: BridgeListing
                let target = ExtensionAdapter.enumerationTarget(
                    for: parentIdentifier == .rootContainer
                        ? .rootContainer
                        : .opaque(parentIdentifier.rawValue)
                )
                switch target {
                case .root:
                    listing = try await bridgeSession.listRoot()
                case let .children(parentItemId):
                    listing = try await bridgeSession.listChildren(parentItemId: parentItemId)
                }

                for message in ExtensionAdapter.warningLogMessages(for: listing.warnings) {
                    logger.warning("\(message, privacy: .public)")
                }

                let items = listing.items.map { snapshot in
                    if snapshot.itemId == "root" {
                        return SampleFileProviderItem(rootDisplayName: domainDisplayName)
                    }
                    return SampleFileProviderItem(snapshot: snapshot)
                }
                observer.value.didEnumerate(items)
                observer.value.finishEnumerating(upTo: nil)
            } catch {
                observer.value.finishEnumeratingWithError(sampleNSError(from: error))
            }
        }
    }

    func enumerateChanges(
        for observer: NSFileProviderChangeObserver,
        from syncAnchor: NSFileProviderSyncAnchor
    ) {
        observer.finishEnumeratingChanges(
            upTo: NSFileProviderSyncAnchor(Data()),
            moreComing: false
        )
    }

    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        completionHandler(NSFileProviderSyncAnchor(Data()))
    }
}
