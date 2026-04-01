import FileProvider
import Foundation

struct FileProviderDomainRegistryClient: SampleDomainRegistryManaging {
    func existingDomains() async throws -> [SampleDomainDescriptor] {
        try await loadDomainDescriptors()
    }

    func addDomain(_ descriptor: SampleDomainDescriptor) async throws {
        let domain = NSFileProviderDomain(
            identifier: NSFileProviderDomainIdentifier(rawValue: descriptor.identifier),
            displayName: descriptor.displayName
        )
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            NSFileProviderManager.add(domain) { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    func removeDomain(identifier: String) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }

                guard let domain = domains.first(where: { $0.identifier.rawValue == identifier }) else {
                    continuation.resume()
                    return
                }

                NSFileProviderManager.remove(domain) { error in
                    if let error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                }
            }
        }
    }

    private func loadDomainDescriptors() async throws -> [SampleDomainDescriptor] {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<[SampleDomainDescriptor], Error>) in
            NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(
                        returning: domains.map {
                            SampleDomainDescriptor(
                                identifier: $0.identifier.rawValue,
                                displayName: $0.displayName
                            )
                        }
                    )
                }
            }
        }
    }
}
