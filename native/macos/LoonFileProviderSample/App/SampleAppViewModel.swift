import AppKit
import Combine
import Foundation

@MainActor
final class SampleAppViewModel: ObservableObject {
    private let configLocator: AppGroupConfigLocator
    private let registry: any SampleDomainRegistryManaging
    private let domainLogic = SampleDomainRegistrationLogic()

    @Published var configFilePath: String = ""
    @Published var configStatusText: String = "Loading sample config..."
    @Published var domainStatusText: String = "Checking domain registration..."
    @Published var errorText: String?
    @Published var isBusy = false

    init(
        configLocator: AppGroupConfigLocator = AppGroupConfigLocator(),
        registry: any SampleDomainRegistryManaging = FileProviderDomainRegistryClient()
    ) {
        self.configLocator = configLocator
        self.registry = registry
    }

    func refresh() async {
        isBusy = true
        defer { isBusy = false }

        do {
            let store = try configLocator.configStore()
            let configURL = try store.ensureDefaultConfigFile()
            configFilePath = configURL.path
            let status = store.loadStatus()
            try syncSharedConfig(for: status)
            configStatusText = describeConfigStatus(status)
            errorText = status.decodeErrorDescription

            guard let config = status.parsedConfig, status.isValid else {
                domainStatusText = "Domain registration is blocked until the config validates."
                return
            }

            let registration = try await domainLogic.status(for: config, registry: registry)
            domainStatusText = registration.isRegistered
                ? "Registered domain: \(registration.descriptor.displayName) (\(registration.descriptor.identifier))"
                : "Domain not registered: \(registration.descriptor.displayName) (\(registration.descriptor.identifier))"
        } catch {
            errorText = describeSampleError(error)
            domainStatusText = "Domain registration unavailable."
        }
    }

    func registerDomain() async {
        isBusy = true
        defer { isBusy = false }

        do {
            let store = try configLocator.configStore()
            _ = try store.ensureDefaultConfigFile()
            let status = store.loadStatus()
            try syncSharedConfig(for: status)
            configStatusText = describeConfigStatus(status)
            guard let config = status.parsedConfig else {
                throw sampleNSError(
                    from: SampleDomainRegistrationError.invalidConfig(
                        [status.decodeErrorDescription ?? "Config could not be parsed."]
                    )
                )
            }
            _ = try await domainLogic.registerIfMissing(config: config, registry: registry)
            errorText = nil
            await refresh()
        } catch {
            errorText = describeSampleError(error)
        }
    }

    func removeDomain() async {
        isBusy = true
        defer { isBusy = false }

        do {
            let status = try configLocator.configStore().loadStatus()
            guard let config = status.parsedConfig else {
                throw sampleNSError(
                    from: SampleDomainRegistrationError.invalidConfig(
                        [status.decodeErrorDescription ?? "Config could not be parsed."]
                    )
                )
            }
            _ = try await domainLogic.removeIfPresent(config: config, registry: registry)
            errorText = nil
            await refresh()
        } catch {
            errorText = describeSampleError(error)
        }
    }

    func revealConfigInFinder() {
        guard !configFilePath.isEmpty else {
            return
        }
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: configFilePath)])
    }

    private func describeConfigStatus(_ status: SampleConfigStatus) -> String {
        if let decodeErrorDescription = status.decodeErrorDescription {
            return "Config parse failed: \(decodeErrorDescription)"
        }
        guard let parsedConfig = status.parsedConfig else {
            return "Config file missing."
        }
        if status.validationErrors.isEmpty {
            return "Config is valid for \(parsedConfig.exposedNamespaces.count) namespace(s)."
        }
        return "Config needs edits:\n" + status.validationErrors.joined(separator: "\n")
    }

    private func syncSharedConfig(for status: SampleConfigStatus) throws {
        let sharedStore = try configLocator.sharedConfigStore()
        guard status.isValid, let config = status.parsedConfig else {
            sharedStore.clear()
            return
        }
        try sharedStore.save(config)
    }
}
