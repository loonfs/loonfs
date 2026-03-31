import FileProvider
import Foundation
import OSLog

final class LoonFileProviderSampleExtension: NSObject, NSFileProviderReplicatedExtension {
    private let domain: NSFileProviderDomain
    private let bridgeSession: SampleBridgeSession
    private let logger = Logger(
        subsystem: SampleDefaults.extensionBundleIdentifier,
        category: "FileProvider"
    )

    required init(domain: NSFileProviderDomain) {
        self.domain = domain
        self.bridgeSession = SampleBridgeSession()
        super.init()
    }

    func invalidate() {
        Task { await bridgeSession.invalidate() }
    }

    func enumerator(
        for containerItemIdentifier: NSFileProviderItemIdentifier,
        request: NSFileProviderRequest
    ) throws -> NSFileProviderEnumerator {
        SampleFileProviderEnumerator(
            parentIdentifier: containerItemIdentifier,
            bridgeSession: bridgeSession,
            logger: logger,
            domainDisplayName: domain.displayName
        )
    }

    func item(
        for identifier: NSFileProviderItemIdentifier,
        request: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        Task {
            do {
                if identifier == .rootContainer {
                    completionHandler(SampleFileProviderItem(rootDisplayName: domain.displayName), nil)
                    return
                }

                let lookup = try await bridgeSession.lookupItem(itemId: identifier.rawValue)
                for message in ExtensionAdapter.warningLogMessages(for: lookup.warnings) {
                    logger.warning("\(message, privacy: .public)")
                }
                guard let item = lookup.item else {
                    let error = NSError(
                        domain: NSFileProviderErrorDomain,
                        code: NSFileProviderError.noSuchItem.rawValue
                    )
                    completionHandler(nil, error)
                    return
                }
                completionHandler(SampleFileProviderItem(snapshot: item), nil)
            } catch {
                completionHandler(nil, sampleNSError(from: error))
            }
        }
        return progress
    }

    func fetchContents(
        for itemIdentifier: NSFileProviderItemIdentifier,
        version requestedVersion: NSFileProviderItemVersion?,
        request: NSFileProviderRequest,
        completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        Task {
            do {
                let bridgeItemId = itemIdentifier.rawValue
                let materialized = try await bridgeSession.materializeItem(
                    itemId: bridgeItemId,
                    nowMs: UInt64(Date().timeIntervalSince1970 * 1000)
                )
                let refreshed = try await bridgeSession.lookupItem(itemId: bridgeItemId)
                guard let item = refreshed.item else {
                    completionHandler(
                        URL(fileURLWithPath: materialized.absolutePath),
                        nil,
                        NSError(
                            domain: NSFileProviderErrorDomain,
                            code: NSFileProviderError.noSuchItem.rawValue
                        )
                    )
                    return
                }
                completionHandler(
                    URL(fileURLWithPath: materialized.absolutePath),
                    SampleFileProviderItem(snapshot: item),
                    nil
                )
            } catch let error as BridgeInteropError {
                completionHandler(nil, nil, sampleNSError(from: ExtensionAdapter.mapBridgeError(error)))
            } catch {
                completionHandler(nil, nil, sampleNSError(from: error))
            }
        }
        return progress
    }
}
