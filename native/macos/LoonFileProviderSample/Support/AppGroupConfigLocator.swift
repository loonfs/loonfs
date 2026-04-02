import Foundation

enum AppGroupConfigLocatorError: LocalizedError {
    case containerUnavailable(String)

    var errorDescription: String? {
        switch self {
        case let .containerUnavailable(identifier):
            return "App Group container unavailable for \(identifier)."
        }
    }
}

struct AppGroupConfigLocator {
    let fileManager: FileManager
    let appGroupIdentifier: String

    init(
        fileManager: FileManager = .default,
        appGroupIdentifier: String = SampleDefaults.appGroupIdentifier
    ) {
        self.fileManager = fileManager
        self.appGroupIdentifier = appGroupIdentifier
    }

    func containerDirectoryURL() throws -> URL {
        guard let url = fileManager.containerURL(
            forSecurityApplicationGroupIdentifier: appGroupIdentifier
        ) else {
            throw AppGroupConfigLocatorError.containerUnavailable(appGroupIdentifier)
        }
        return url
    }

    func supportDirectoryURL() throws -> URL {
        try containerDirectoryURL()
            .appendingPathComponent("Library", isDirectory: true)
            .appendingPathComponent("Application Support", isDirectory: true)
            .appendingPathComponent(
                SampleDefaults.appGroupSupportDirectoryName,
                isDirectory: true
            )
    }

    func configStore() throws -> SampleConfigStore {
        SampleConfigStore(configDirectoryURL: try supportDirectoryURL())
    }

    func sharedConfigStore() throws -> SampleSharedConfigStore {
        try SampleSharedConfigStore(appGroupIdentifier: appGroupIdentifier)
    }
}
