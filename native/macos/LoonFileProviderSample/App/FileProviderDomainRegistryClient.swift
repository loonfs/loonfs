import FileProvider
import Foundation

struct FileProviderDomainRegistryClient: SampleDomainRegistryManaging {
    func existingDomains() async throws -> [SampleDomainDescriptor] {
        try await loadDomains().map {
            SampleDomainDescriptor(
                identifier: $0.identifier.rawValue,
                displayName: $0.displayName
            )
        }
    }

    func addDomain(_ descriptor: SampleDomainDescriptor) async throws {
        let domain = NSFileProviderDomain(
            identifier: NSFileProviderDomainIdentifier(rawValue: descriptor.identifier),
            displayName: descriptor.displayName
        )
        try await withCheckedThrowingContinuation { continuation in
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
        guard let domain = try await loadDomains().first(where: { $0.identifier.rawValue == identifier }) else {
            return
        }
        try await withCheckedThrowingContinuation { continuation in
            NSFileProviderManager.remove(domain) { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    private func loadDomains() async throws -> [NSFileProviderDomain] {
        try await withCheckedThrowingContinuation { continuation in
            NSFileProviderManager.getDomainsWithCompletionHandler { domains, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: domains ?? [])
                }
            }
        }
    }
}
