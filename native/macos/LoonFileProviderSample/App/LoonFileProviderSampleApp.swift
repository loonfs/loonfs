import SwiftUI

@main
struct LoonFileProviderSampleApp: App {
    @StateObject private var viewModel = SampleAppViewModel()

    var body: some Scene {
        WindowGroup {
            ContentView(viewModel: viewModel)
        }
    }
}
