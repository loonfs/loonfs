import Foundation

public struct SampleConfig: Codable, Equatable, Sendable {
    public var opsConfigPath: String
    public var exposedNamespaces: [String]
    public var domainIdentifier: String
    public var domainDisplayName: String
    public var appGroupIdentifier: String

    public init(
        opsConfigPath: String,
        exposedNamespaces: [String],
        domainIdentifier: String,
        domainDisplayName: String,
        appGroupIdentifier: String
    ) {
        self.opsConfigPath = opsConfigPath
        self.exposedNamespaces = exposedNamespaces
        self.domainIdentifier = domainIdentifier
        self.domainDisplayName = domainDisplayName
        self.appGroupIdentifier = appGroupIdentifier
    }

    public static var template: Self {
        Self(
            opsConfigPath: "",
            exposedNamespaces: [],
            domainIdentifier: SampleDefaults.domainIdentifier,
            domainDisplayName: SampleDefaults.domainDisplayName,
            appGroupIdentifier: SampleDefaults.appGroupIdentifier
        )
    }

    public func validationErrors(
        expectedAppGroupIdentifier: String = SampleDefaults.appGroupIdentifier
    ) -> [String] {
        var errors: [String] = []
        if opsConfigPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("`ops_config_path` must point at an existing ops config file.")
        }
        if exposedNamespaces.isEmpty {
            errors.append("`exposed_namespaces` must include at least one namespace id.")
        }
        if domainIdentifier.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("`domain_identifier` must not be empty.")
        }
        if domainDisplayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("`domain_display_name` must not be empty.")
        }
        if appGroupIdentifier.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            errors.append("`app_group_identifier` must not be empty.")
        }
        if appGroupIdentifier != expectedAppGroupIdentifier {
            errors.append(
                "`app_group_identifier` must match the sample entitlements (`\(expectedAppGroupIdentifier)`)."
            )
        }
        return errors
    }
}

public struct SampleConfigStatus: Equatable, Sendable {
    public let configFileURL: URL
    public let fileExists: Bool
    public let parsedConfig: SampleConfig?
    public let validationErrors: [String]
    public let decodeErrorDescription: String?

    public init(
        configFileURL: URL,
        fileExists: Bool,
        parsedConfig: SampleConfig?,
        validationErrors: [String],
        decodeErrorDescription: String?
    ) {
        self.configFileURL = configFileURL
        self.fileExists = fileExists
        self.parsedConfig = parsedConfig
        self.validationErrors = validationErrors
        self.decodeErrorDescription = decodeErrorDescription
    }

    public var isValid: Bool {
        fileExists && parsedConfig != nil && validationErrors.isEmpty && decodeErrorDescription == nil
    }
}

public struct SampleConfigStore {
    public let configDirectoryURL: URL
    public let configFileName: String
    private let fileManager: FileManager

    public init(
        configDirectoryURL: URL,
        configFileName: String = SampleDefaults.configFileName,
        fileManager: FileManager = .default
    ) {
        self.configDirectoryURL = configDirectoryURL
        self.configFileName = configFileName
        self.fileManager = fileManager
    }

    public var configFileURL: URL {
        configDirectoryURL.appendingPathComponent(configFileName, isDirectory: false)
    }

    @discardableResult
    public func ensureDefaultConfigFile() throws -> URL {
        try fileManager.createDirectory(
            at: configDirectoryURL,
            withIntermediateDirectories: true,
            attributes: nil
        )
        guard !fileManager.fileExists(atPath: configFileURL.path) else {
            return configFileURL
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(SampleConfig.template)
        try data.write(to: configFileURL, options: .atomic)
        return configFileURL
    }

    public func loadStatus(
        expectedAppGroupIdentifier: String = SampleDefaults.appGroupIdentifier
    ) -> SampleConfigStatus {
        let fileExists = fileManager.fileExists(atPath: configFileURL.path)
        guard fileExists else {
            return SampleConfigStatus(
                configFileURL: configFileURL,
                fileExists: false,
                parsedConfig: nil,
                validationErrors: ["Config file does not exist yet."],
                decodeErrorDescription: nil
            )
        }

        do {
            let data = try Data(contentsOf: configFileURL)
            let decoder = JSONDecoder()
            decoder.keyDecodingStrategy = .convertFromSnakeCase
            let config = try decoder.decode(SampleConfig.self, from: data)
            return SampleConfigStatus(
                configFileURL: configFileURL,
                fileExists: true,
                parsedConfig: config,
                validationErrors: config.validationErrors(
                    expectedAppGroupIdentifier: expectedAppGroupIdentifier
                ),
                decodeErrorDescription: nil
            )
        } catch {
            return SampleConfigStatus(
                configFileURL: configFileURL,
                fileExists: true,
                parsedConfig: nil,
                validationErrors: [],
                decodeErrorDescription: error.localizedDescription
            )
        }
    }
}
