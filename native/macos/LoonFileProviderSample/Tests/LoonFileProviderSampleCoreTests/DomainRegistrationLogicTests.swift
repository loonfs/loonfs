import Testing
@testable import LoonFileProviderSampleCore

struct DomainRegistrationLogicTests {
    @Test
    func registerIfMissingAddsConfiguredDomain() async throws {
        let registry = FakeDomainRegistry()
        let logic = SampleDomainRegistrationLogic()
        let config = SampleConfig(
            opsConfigPath: "/tmp/ops.toml",
            exposedNamespaces: ["demo"],
            domainIdentifier: "main",
            domainDisplayName: "Loon",
            appGroupIdentifier: SampleDefaults.appGroupIdentifier
        )

        let status = try await logic.registerIfMissing(config: config, registry: registry)
        #expect(status.isRegistered)
        #expect(
            registry.addedDomains == [.init(identifier: "main", displayName: "Loon")]
        )
    }

    @Test
    func removeIfPresentRemovesRegisteredDomain() async throws {
        let registry = FakeDomainRegistry(
            existingDomains: [.init(identifier: "main", displayName: "Loon")]
        )
        let logic = SampleDomainRegistrationLogic()
        let config = SampleConfig(
            opsConfigPath: "/tmp/ops.toml",
            exposedNamespaces: ["demo"],
            domainIdentifier: "main",
            domainDisplayName: "Loon",
            appGroupIdentifier: SampleDefaults.appGroupIdentifier
        )

        let removed = try await logic.removeIfPresent(config: config, registry: registry)
        #expect(removed)
        #expect(registry.removedDomainIdentifiers == ["main"])
    }

    @Test
    func invalidConfigBlocksRegistration() async throws {
        let registry = FakeDomainRegistry()
        let logic = SampleDomainRegistrationLogic()
        let config = SampleConfig.template

        do {
            _ = try await logic.registerIfMissing(config: config, registry: registry)
            Issue.record("expected invalid config error")
        } catch let error as SampleDomainRegistrationError {
            switch error {
            case let .invalidConfig(errors):
                #expect(
                    errors.contains("`ops_config_path` must point at an existing ops config file.")
                )
            }
        }
    }
}

private final class FakeDomainRegistry: @unchecked Sendable, SampleDomainRegistryManaging {
    private(set) var addedDomains: [SampleDomainDescriptor] = []
    private(set) var removedDomainIdentifiers: [String] = []
    private var storedDomains: [SampleDomainDescriptor]

    init(existingDomains: [SampleDomainDescriptor] = []) {
        self.storedDomains = existingDomains
    }

    func existingDomains() async throws -> [SampleDomainDescriptor] {
        storedDomains
    }

    func addDomain(_ descriptor: SampleDomainDescriptor) async throws {
        addedDomains.append(descriptor)
        storedDomains.append(descriptor)
    }

    func removeDomain(identifier: String) async throws {
        removedDomainIdentifiers.append(identifier)
        storedDomains.removeAll { $0.identifier == identifier }
    }
}
