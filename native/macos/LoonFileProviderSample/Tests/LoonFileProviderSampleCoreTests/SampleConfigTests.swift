import Foundation
import Testing
@testable import LoonFileProviderSampleCore

struct SampleConfigTests {
    @Test
    func ensureDefaultConfigFileCreatesTemplate() throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let store = SampleConfigStore(configDirectoryURL: tempDirectory)

        let configURL = try store.ensureDefaultConfigFile()
        let status = store.loadStatus()

        #expect(configURL == status.configFileURL)
        #expect(status.fileExists)
        #expect(status.parsedConfig == SampleConfig.template)
        #expect(!status.isValid)
        #expect(
            status.validationErrors == [
                "`ops_config_path` must point at an existing ops config file.",
                "`exposed_namespaces` must include at least one namespace id.",
            ]
        )
    }

    @Test
    func loadStatusParsesValidConfig() throws {
        let tempDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(
            at: tempDirectory,
            withIntermediateDirectories: true,
            attributes: nil
        )
        let store = SampleConfigStore(configDirectoryURL: tempDirectory)
        let config = SampleConfig(
            opsConfigPath: "/tmp/loondb-provider.toml",
            exposedNamespaces: ["demo"],
            domainIdentifier: "main",
            domainDisplayName: "Loon",
            appGroupIdentifier: SampleDefaults.appGroupIdentifier
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        try encoder.encode(config).write(to: store.configFileURL, options: .atomic)

        let status = store.loadStatus()
        #expect(status.isValid)
        #expect(status.parsedConfig == config)
        #expect(status.decodeErrorDescription == nil)
    }
}
