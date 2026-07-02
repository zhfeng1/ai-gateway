import Cocoa
import WebKit

private let defaultPort = "20000"

final class LauncherAppDelegate: NSObject, NSApplicationDelegate, WKNavigationDelegate {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var startView: NSView!
    private var portField: NSTextField!
    private var statusLabel: NSTextField!
    private var startButton: NSButton!
    private var backendProcess: Process?

    func applicationDidFinishLaunching(_ notification: Notification) {
        buildWindow()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationWillTerminate(_ notification: Notification) {
        stopBackend()
    }

    private func buildWindow() {
        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1280, height: 860),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "AI Gateway"
        window.minSize = NSSize(width: 920, height: 640)
        window.center()

        webView = WKWebView(frame: .zero)
        webView.navigationDelegate = self

        startView = makeStartView()
        window.contentView = startView
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    private func makeStartView() -> NSView {
        let root = NSView()
        root.wantsLayer = true
        root.layer?.backgroundColor = NSColor(red: 0.031, green: 0.043, blue: 0.063, alpha: 1).cgColor

        let panel = NSView()
        panel.translatesAutoresizingMaskIntoConstraints = false
        panel.wantsLayer = true
        panel.layer?.backgroundColor = NSColor(red: 0.063, green: 0.082, blue: 0.122, alpha: 1).cgColor
        panel.layer?.cornerRadius = 14
        panel.layer?.borderWidth = 1
        panel.layer?.borderColor = NSColor(red: 0.145, green: 0.188, blue: 0.267, alpha: 1).cgColor
        root.addSubview(panel)

        let mark = NSTextField(labelWithString: "AI")
        mark.translatesAutoresizingMaskIntoConstraints = false
        mark.alignment = .center
        mark.font = .systemFont(ofSize: 16, weight: .bold)
        mark.textColor = NSColor(red: 0.153, green: 0.82, blue: 0.498, alpha: 1)
        mark.wantsLayer = true
        mark.layer?.backgroundColor = NSColor(red: 0.043, green: 0.125, blue: 0.09, alpha: 1).cgColor
        mark.layer?.cornerRadius = 10
        panel.addSubview(mark)

        let title = NSTextField(labelWithString: "AI Gateway")
        title.translatesAutoresizingMaskIntoConstraints = false
        title.font = .systemFont(ofSize: 24, weight: .semibold)
        title.textColor = .white
        panel.addSubview(title)

        let subtitle = NSTextField(wrappingLabelWithString: "选择本地端口后，将在此窗口内打开网关控制台。")
        subtitle.translatesAutoresizingMaskIntoConstraints = false
        subtitle.font = .systemFont(ofSize: 14)
        subtitle.textColor = NSColor(red: 0.592, green: 0.643, blue: 0.722, alpha: 1)
        panel.addSubview(subtitle)

        let label = NSTextField(labelWithString: "启动端口")
        label.translatesAutoresizingMaskIntoConstraints = false
        label.font = .systemFont(ofSize: 13, weight: .semibold)
        label.textColor = NSColor(red: 0.592, green: 0.643, blue: 0.722, alpha: 1)
        panel.addSubview(label)

        portField = NSTextField(string: defaultPort)
        portField.translatesAutoresizingMaskIntoConstraints = false
        portField.font = .systemFont(ofSize: 15)
        portField.focusRingType = .none
        panel.addSubview(portField)

        startButton = NSButton(title: "启动", target: self, action: #selector(startClicked))
        startButton.translatesAutoresizingMaskIntoConstraints = false
        startButton.bezelStyle = .rounded
        startButton.keyEquivalent = "\r"
        panel.addSubview(startButton)

        statusLabel = NSTextField(wrappingLabelWithString: "")
        statusLabel.translatesAutoresizingMaskIntoConstraints = false
        statusLabel.font = .systemFont(ofSize: 13)
        statusLabel.textColor = NSColor(red: 1.0, green: 0.361, blue: 0.451, alpha: 1)
        panel.addSubview(statusLabel)

        NSLayoutConstraint.activate([
            panel.centerXAnchor.constraint(equalTo: root.centerXAnchor),
            panel.centerYAnchor.constraint(equalTo: root.centerYAnchor),
            panel.widthAnchor.constraint(equalToConstant: 460),

            mark.topAnchor.constraint(equalTo: panel.topAnchor, constant: 28),
            mark.leadingAnchor.constraint(equalTo: panel.leadingAnchor, constant: 28),
            mark.widthAnchor.constraint(equalToConstant: 44),
            mark.heightAnchor.constraint(equalToConstant: 44),

            title.topAnchor.constraint(equalTo: mark.bottomAnchor, constant: 18),
            title.leadingAnchor.constraint(equalTo: panel.leadingAnchor, constant: 28),
            title.trailingAnchor.constraint(equalTo: panel.trailingAnchor, constant: -28),

            subtitle.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 6),
            subtitle.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            subtitle.trailingAnchor.constraint(equalTo: title.trailingAnchor),

            label.topAnchor.constraint(equalTo: subtitle.bottomAnchor, constant: 22),
            label.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            label.trailingAnchor.constraint(equalTo: title.trailingAnchor),

            portField.topAnchor.constraint(equalTo: label.bottomAnchor, constant: 8),
            portField.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            portField.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            portField.heightAnchor.constraint(equalToConstant: 38),

            startButton.topAnchor.constraint(equalTo: portField.bottomAnchor, constant: 14),
            startButton.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            startButton.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            startButton.heightAnchor.constraint(equalToConstant: 38),

            statusLabel.topAnchor.constraint(equalTo: startButton.bottomAnchor, constant: 12),
            statusLabel.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            statusLabel.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            statusLabel.bottomAnchor.constraint(equalTo: panel.bottomAnchor, constant: -28)
        ])

        DispatchQueue.main.async { self.window.makeFirstResponder(self.portField) }
        return root
    }

    @objc private func startClicked() {
        let rawPort = portField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let port = rawPort.isEmpty ? defaultPort : rawPort

        guard let portValue = Int(port), (1...65535).contains(portValue) else {
            setStatus("请输入有效的数字端口。", isError: true)
            return
        }

        guard isPortAvailable(portValue) else {
            setStatus("端口 \(portValue) 已被占用，请换一个端口。", isError: true)
            return
        }

        startButton.isEnabled = false
        setStatus("启动中...", isError: false)

        do {
            try startBackend(port: port)
        } catch {
            startButton.isEnabled = true
            setStatus("启动失败：\(error.localizedDescription)", isError: true)
            return
        }

        waitForServer(port: portValue) { ok in
            DispatchQueue.main.async {
                if ok {
                    self.loadDashboard(port: portValue)
                } else {
                    self.stopBackend()
                    self.startButton.isEnabled = true
                    self.setStatus("服务启动超时，请重试。", isError: true)
                }
            }
        }
    }

    private func startBackend(port: String) throws {
        stopBackend()

        guard let resources = Bundle.main.resourceURL else {
            throw LauncherError.missingResources
        }

        let executable = resources
            .appendingPathComponent("backend")
            .appendingPathComponent("ai-gateway-backend")

        guard FileManager.default.isExecutableFile(atPath: executable.path) else {
            throw LauncherError.missingBackend
        }

        let process = Process()
        process.executableURL = executable
        process.arguments = ["--host", "127.0.0.1", "--port", port, "--no-browser"]
        process.currentDirectoryURL = executable.deletingLastPathComponent()
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        backendProcess = process
    }

    private func loadDashboard(port: Int) {
        let url = URL(string: "http://127.0.0.1:\(port)/")!
        webView.frame = window.contentView?.bounds ?? .zero
        webView.autoresizingMask = [.width, .height]
        webView.load(URLRequest(url: url))
        window.contentView = webView
    }

    private func stopBackend() {
        if let process = backendProcess, process.isRunning {
            process.terminate()
        }
        backendProcess = nil
    }

    private func setStatus(_ message: String, isError: Bool) {
        statusLabel.stringValue = message
        statusLabel.textColor = isError
            ? NSColor(red: 1.0, green: 0.361, blue: 0.451, alpha: 1)
            : NSColor(red: 0.153, green: 0.82, blue: 0.498, alpha: 1)
    }

    private func isPortAvailable(_ port: Int) -> Bool {
        var hints = addrinfo(
            ai_flags: AI_PASSIVE,
            ai_family: AF_INET,
            ai_socktype: SOCK_STREAM,
            ai_protocol: 0,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )
        var result: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo("127.0.0.1", String(port), &hints, &result) == 0, let info = result else {
            return false
        }
        defer { freeaddrinfo(result) }

        let socketFd = socket(info.pointee.ai_family, info.pointee.ai_socktype, info.pointee.ai_protocol)
        guard socketFd >= 0 else { return false }
        defer { close(socketFd) }

        return connect(socketFd, info.pointee.ai_addr, info.pointee.ai_addrlen) != 0
    }

    private func waitForServer(port: Int, completion: @escaping (Bool) -> Void) {
        DispatchQueue.global(qos: .userInitiated).async {
            let deadline = Date().addingTimeInterval(8)
            while Date() < deadline {
                if !self.isPortAvailable(port) {
                    completion(true)
                    return
                }
                Thread.sleep(forTimeInterval: 0.1)
            }
            completion(false)
        }
    }
}

private enum LauncherError: LocalizedError {
    case missingResources
    case missingBackend

    var errorDescription: String? {
        switch self {
        case .missingResources:
            return "找不到应用资源目录。"
        case .missingBackend:
            return "找不到内置网关服务。"
        }
    }
}

let app = NSApplication.shared
let delegate = LauncherAppDelegate()
app.delegate = delegate
app.setActivationPolicy(.regular)
app.run()
