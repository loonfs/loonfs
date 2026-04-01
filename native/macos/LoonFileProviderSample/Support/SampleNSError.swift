import Foundation

enum SampleErrorCode {
    static let unavailable = 1
    static let bridgeFailure = 2
    static let invalidConfig = 3
    static let readOnly = 4
}

func sampleNSError(from error: Error) -> NSError {
    if let providerError = error as? SampleProviderError {
        switch providerError {
        case let .unavailable(message):
            return NSError(
                domain: "dev.loondb.LoonFileProviderSample",
                code: SampleErrorCode.unavailable,
                userInfo: [NSLocalizedDescriptionKey: message]
            )
        case let .readOnly(message):
            return NSError(
                domain: "dev.loondb.LoonFileProviderSample",
                code: SampleErrorCode.readOnly,
                userInfo: [NSLocalizedDescriptionKey: message]
            )
        case let .bridgeFailure(code, message):
            return NSError(
                domain: "dev.loondb.LoonFileProviderSample",
                code: SampleErrorCode.bridgeFailure,
                userInfo: [
                    NSLocalizedDescriptionKey: message,
                    "bridge_code": code,
                ]
            )
        }
    }

    if let error = error as? SampleDomainRegistrationError {
        switch error {
        case let .invalidConfig(errors):
            return NSError(
                domain: "dev.loondb.LoonFileProviderSample",
                code: SampleErrorCode.invalidConfig,
                userInfo: [NSLocalizedDescriptionKey: errors.joined(separator: "\n")]
            )
        }
    }

    let nsError = error as NSError
    return NSError(
        domain: nsError.domain,
        code: nsError.code,
        userInfo: nsError.userInfo.merging(
            [NSLocalizedDescriptionKey: nsError.localizedDescription],
            uniquingKeysWith: { current, _ in current }
        )
    )
}

func describeSampleError(_ error: Error) -> String {
    let nsError = error as NSError
    var lines = [nsError.localizedDescription]
    lines.append("domain=\(nsError.domain) code=\(nsError.code)")

    if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? NSError {
        lines.append("underlying=\(underlying.domain) code=\(underlying.code)")
        if !underlying.localizedDescription.isEmpty, underlying.localizedDescription != nsError.localizedDescription {
            lines.append("underlying_description=\(underlying.localizedDescription)")
        }
    }

    if let bridgeCode = nsError.userInfo["bridge_code"] {
        lines.append("bridge_code=\(bridgeCode)")
    }

    return lines.joined(separator: "\n")
}
