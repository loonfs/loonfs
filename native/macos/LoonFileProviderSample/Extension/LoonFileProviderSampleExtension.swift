import FileProvider
import Foundation
import OSLog

private struct SendableBox<T>: @unchecked Sendable {
    let value: T
}

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
        let bridgeSession = self.bridgeSession
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
        let bridgeSession = self.bridgeSession
        let logger = self.logger
        let domainDisplayName = domain.displayName
        let completion = SendableBox(value: completionHandler)
        Task {
            do {
                if identifier == .rootContainer {
                    completion.value(SampleFileProviderItem(rootDisplayName: domainDisplayName), nil)
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
                    completion.value(nil, error)
                    return
                }
                completion.value(SampleFileProviderItem(snapshot: item), nil)
            } catch {
                completion.value(nil, sampleNSError(from: error))
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
        let bridgeSession = self.bridgeSession
        let completion = SendableBox(value: completionHandler)
        Task {
            do {
                let bridgeItemId = itemIdentifier.rawValue
                let materialized = try await bridgeSession.materializeItem(
                    itemId: bridgeItemId,
                    nowMs: UInt64(Date().timeIntervalSince1970 * 1000)
                )
                let refreshed = try await bridgeSession.lookupItem(itemId: bridgeItemId)
                guard let item = refreshed.item else {
                    completion.value(
                        URL(fileURLWithPath: materialized.absolutePath),
                        nil,
                        NSError(
                            domain: NSFileProviderErrorDomain,
                            code: NSFileProviderError.noSuchItem.rawValue
                        )
                    )
                    return
                }
                completion.value(
                    URL(fileURLWithPath: materialized.absolutePath),
                    SampleFileProviderItem(snapshot: item),
                    nil
                )
            } catch let error as BridgeInteropError {
                completion.value(nil, nil, sampleNSError(from: ExtensionAdapter.mapBridgeError(error)))
            } catch {
                completion.value(nil, nil, sampleNSError(from: error))
            }
        }
        return progress
    }

    func createItem(
        basedOn itemTemplate: NSFileProviderItem,
        fields: NSFileProviderItemFields,
        contents url: URL?,
        options: NSFileProviderCreateItemOptions = [],
        request: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        completionHandler(
            nil,
            [],
            false,
            sampleNSError(from: SampleProviderError.readOnly("The sample File Provider is read-only."))
        )
        return progress
    }

    func modifyItem(
        _ item: NSFileProviderItem,
        baseVersion version: NSFileProviderItemVersion,
        changedFields: NSFileProviderItemFields,
        contents newContents: URL?,
        options: NSFileProviderModifyItemOptions = [],
        request: NSFileProviderRequest,
        completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        completionHandler(
            nil,
            [],
            false,
            sampleNSError(from: SampleProviderError.readOnly("The sample File Provider is read-only."))
        )
        return progress
    }

    func deleteItem(
        identifier: NSFileProviderItemIdentifier,
        baseVersion version: NSFileProviderItemVersion,
        options: NSFileProviderDeleteItemOptions = [],
        request: NSFileProviderRequest,
        completionHandler: @escaping (Error?) -> Void
    ) -> Progress {
        let progress = Progress(totalUnitCount: 1)
        completionHandler(
            sampleNSError(from: SampleProviderError.readOnly("The sample File Provider is read-only."))
        )
        return progress
    }
}
