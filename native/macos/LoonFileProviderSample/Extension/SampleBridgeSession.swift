import Foundation

actor SampleBridgeSession {
    private let configLocator: AppGroupConfigLocator
    private var cachedConfig: SampleConfig?
    private var bridgeClient: BridgeClient?

    init(configLocator: AppGroupConfigLocator = AppGroupConfigLocator()) {
        self.configLocator = configLocator
    }

    func loadCurrentConfig() throws -> SampleConfig {
        let store = try configLocator.configStore()
        let status = store.loadStatus()
        if let decodeErrorDescription = status.decodeErrorDescription {
            throw SampleDomainRegistrationError.invalidConfig([decodeErrorDescription])
        }
        guard let config = status.parsedConfig else {
            throw SampleDomainRegistrationError.invalidConfig(["Sample config is missing."])
        }
        let validationErrors = config.validationErrors()
        if !validationErrors.isEmpty {
            throw SampleDomainRegistrationError.invalidConfig(validationErrors)
        }
        return config
    }

    func currentConfig() throws -> SampleConfig {
        if let cachedConfig {
            return cachedConfig
        }
        let config = try loadCurrentConfig()
        cachedConfig = config
        return config
    }

    func listRoot() throws -> BridgeListing {
        let client = try openClientIfNeeded()
        return try client.listRoot()
    }

    func lookupItem(itemId: String) throws -> BridgeLookupResult {
        let client = try openClientIfNeeded()
        return try client.lookupItem(itemId: itemId)
    }

    func listChildren(parentItemId: String) throws -> BridgeListing {
        let client = try openClientIfNeeded()
        return try client.listChildren(parentItemId: parentItemId)
    }

    func materializeItem(itemId: String, nowMs: UInt64) throws -> BridgeMaterializedPath {
        let client = try openClientIfNeeded()
        return try client.materializeItem(itemId: itemId, nowMs: nowMs)
    }

    func invalidate() {
        try? bridgeClient?.close()
        bridgeClient = nil
        cachedConfig = nil
    }

    private func openClientIfNeeded() throws -> BridgeClient {
        if let bridgeClient {
            return bridgeClient
        }
        let config = try currentConfig()
        let client = BridgeClient(functions: LoonMacosFFIFunctionCaller())
        _ = try client.open(config: config)
        bridgeClient = client
        return client
    }
}
