// swift-tools-version: 6.1

import PackageDescription

let package = Package(
    name: "LoonFileProviderSample",
    platforms: [
        .macOS(.v15),
    ],
    products: [
        .library(
            name: "LoonFileProviderSampleCore",
            targets: ["LoonFileProviderSampleCore"]
        ),
    ],
    targets: [
        .target(
            name: "LoonFileProviderSampleCore",
            path: "Sources/LoonFileProviderSampleCore"
        ),
        .testTarget(
            name: "LoonFileProviderSampleCoreTests",
            dependencies: ["LoonFileProviderSampleCore"],
            path: "Tests/LoonFileProviderSampleCoreTests"
        ),
    ]
)
