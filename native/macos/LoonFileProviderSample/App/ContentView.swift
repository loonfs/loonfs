import SwiftUI

struct ContentView: View {
    @ObservedObject var viewModel: SampleAppViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Loon File Provider Sample")
                .font(.title2.weight(.semibold))

            GroupBox("Sample Config") {
                VStack(alignment: .leading, spacing: 8) {
                    Text(viewModel.configFilePath.isEmpty ? "Loading…" : viewModel.configFilePath)
                        .font(.system(.body, design: .monospaced))
                        .textSelection(.enabled)
                    Text(viewModel.configStatusText)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            GroupBox("Domain Registration") {
                Text(viewModel.domainStatusText)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }

            if let errorText = viewModel.errorText {
                GroupBox("Last Error") {
                    Text(errorText)
                        .foregroundStyle(.red)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }

            HStack {
                Button("Register Domain") {
                    Task { await viewModel.registerDomain() }
                }
                .disabled(viewModel.isBusy)

                Button("Remove Domain") {
                    Task { await viewModel.removeDomain() }
                }
                .disabled(viewModel.isBusy)

                Button("Reveal Config") {
                    viewModel.revealConfigInFinder()
                }
                .disabled(viewModel.configFilePath.isEmpty)
            }
        }
        .padding(24)
        .frame(minWidth: 640, minHeight: 360)
        .task {
            await viewModel.refresh()
        }
    }
}
