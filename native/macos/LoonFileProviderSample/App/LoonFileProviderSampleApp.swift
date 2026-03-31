import SwiftUI

@main
struct LoonFileProviderSampleApp: App {
    @State private var viewModel = SampleAppViewModel()

    var body: some Scene {
        WindowGroup {
            ContentView(viewModel: viewModel)
        }
    }
}
