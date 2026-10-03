// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "RymeDB",
    platforms: [.macOS(.13), .iOS(.16)],
    products: [.library(name: "RymeDB", targets: ["RymeDB"])],
    targets: [.target(name: "RymeDB")]
)
