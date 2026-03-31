import Foundation

public struct SampleDomainDescriptor: Equatable {
    public let identifier: String
    public let displayName: String

    public init(identifier: String, displayName: String) {
        self.identifier = identifier
        self.displayName = displayName
    }
}

public struct SampleDomainRegistrationStatus: Equatable {
    public let descriptor: SampleDomainDescriptor
    public let isRegistered: Bool

    public init(descriptor: SampleDomainDescriptor, isRegistered: Bool) {
        self.descriptor = descriptor
        self.isRegistered = isRegistered
    }
}

public protocol SampleDomainRegistryManaging {
    func existingDomains() async throws -> [SampleDomainDescriptor]
    func addDomain(_ descriptor: SampleDomainDescriptor) async throws
    func removeDomain(identifier: String) async throws
}

public enum SampleDomainRegistrationError: Error, Equatable {
    case invalidConfig([String])
}

public struct SampleDomainRegistrationLogic {
    public init() {}

    public func descriptor(for config: SampleConfig) -> SampleDomainDescriptor {
        SampleDomainDescriptor(
            identifier: config.domainIdentifier,
            displayName: config.domainDisplayName
        )
    }

    public func status(
        for config: SampleConfig,
        registry: SampleDomainRegistryManaging
    ) async throws -> SampleDomainRegistrationStatus {
        let descriptor = descriptor(for: config)
        let domains = try await registry.existingDomains()
        let isRegistered = domains.contains(where: { $0.identifier == descriptor.identifier })
        return SampleDomainRegistrationStatus(descriptor: descriptor, isRegistered: isRegistered)
    }

    @discardableResult
    public func registerIfMissing(
        config: SampleConfig,
        registry: SampleDomainRegistryManaging
    ) async throws -> SampleDomainRegistrationStatus {
        let validationErrors = config.validationErrors()
        if !validationErrors.isEmpty {
            throw SampleDomainRegistrationError.invalidConfig(validationErrors)
        }

        let descriptor = descriptor(for: config)
        let domains = try await registry.existingDomains()
        if !domains.contains(where: { $0.identifier == descriptor.identifier }) {
            try await registry.addDomain(descriptor)
        }
        return SampleDomainRegistrationStatus(descriptor: descriptor, isRegistered: true)
    }

    @discardableResult
    public func removeIfPresent(
        config: SampleConfig,
        registry: SampleDomainRegistryManaging
    ) async throws -> Bool {
        let descriptor = descriptor(for: config)
        let domains = try await registry.existingDomains()
        guard domains.contains(where: { $0.identifier == descriptor.identifier }) else {
            return false
        }
        try await registry.removeDomain(identifier: descriptor.identifier)
        return true
    }
}
