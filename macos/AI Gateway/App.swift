import Cocoa
import WebKit

private let defaultPort = "20000"

private func tr(_ key: String) -> String {
    NSLocalizedString(key, tableName: nil, bundle: .main, value: key, comment: "")
}

final class LauncherAppDelegate: NSObject, NSApplicationDelegate, NSWindowDelegate, NSToolbarDelegate, WKNavigationDelegate {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var startView: NSView!
    private var toolbar: NSToolbar!
    private var portField: NSTextField!
    private var statusLabel: NSTextField!
    private var startButton: NSButton!
    private var backendProcess: Process?

    func applicationDidFinishLaunching(_ notification: Notification) {
        buildMenu()
        buildWindow()
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        stopBackend(force: true)
        return .terminateNow
    }

    func applicationWillTerminate(_ notification: Notification) {
        stopBackend(force: true)
    }

    func windowWillClose(_ notification: Notification) {
        NSApp.terminate(nil)
    }

    private func buildMenu() {
        let mainMenu = NSMenu()
        let appMenuItem = NSMenuItem()
        let appMenu = NSMenu()
        let quitTitle = String(format: tr("menu.quit"), tr("app.name"))
        let quitItem = NSMenuItem(title: quitTitle, action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")

        quitItem.target = NSApp
        appMenu.addItem(quitItem)
        appMenuItem.submenu = appMenu
        mainMenu.addItem(appMenuItem)
        NSApp.mainMenu = mainMenu
    }

    private func buildWindow() {
        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1280, height: 860),
            styleMask: [.titled, .closable, .miniaturizable],
            backing: .buffered,
            defer: false
        )
        window.delegate = self
        window.title = tr("app.name")
        window.minSize = NSSize(width: 1280, height: 860)
        window.maxSize = NSSize(width: 1280, height: 860)
        window.center()

        toolbar = NSToolbar(identifier: "mainToolbar")
        toolbar.delegate = self
        toolbar.displayMode = .iconAndLabel
        toolbar.allowsUserCustomization = false

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

        let title = NSTextField(labelWithString: tr("app.name"))
        title.translatesAutoresizingMaskIntoConstraints = false
        title.font = .systemFont(ofSize: 24, weight: .semibold)
        title.textColor = .white
        panel.addSubview(title)

        let badge = NSTextField(labelWithString: tr("start.badge"))
        badge.translatesAutoresizingMaskIntoConstraints = false
        badge.alignment = .center
        badge.font = .systemFont(ofSize: 12, weight: .semibold)
        badge.textColor = NSColor(red: 0.153, green: 0.82, blue: 0.498, alpha: 1)
        badge.wantsLayer = true
        badge.layer?.backgroundColor = NSColor(red: 0.043, green: 0.125, blue: 0.09, alpha: 1).cgColor
        badge.layer?.cornerRadius = 9
        badge.layer?.borderWidth = 1
        badge.layer?.borderColor = NSColor(red: 0.153, green: 0.82, blue: 0.498, alpha: 0.35).cgColor
        panel.addSubview(badge)

        let subtitle = NSTextField(wrappingLabelWithString: tr("start.subtitle"))
        subtitle.translatesAutoresizingMaskIntoConstraints = false
        subtitle.font = .systemFont(ofSize: 14)
        subtitle.textColor = NSColor(red: 0.592, green: 0.643, blue: 0.722, alpha: 1)
        panel.addSubview(subtitle)

        let hintBox = NSView()
        hintBox.translatesAutoresizingMaskIntoConstraints = false
        hintBox.wantsLayer = true
        hintBox.layer?.backgroundColor = NSColor(red: 0.035, green: 0.052, blue: 0.078, alpha: 1).cgColor
        hintBox.layer?.cornerRadius = 10
        hintBox.layer?.borderWidth = 1
        hintBox.layer?.borderColor = NSColor(red: 0.145, green: 0.188, blue: 0.267, alpha: 0.8).cgColor
        panel.addSubview(hintBox)

        let hintTitle = NSTextField(labelWithString: tr("start.hintTitle"))
        hintTitle.translatesAutoresizingMaskIntoConstraints = false
        hintTitle.font = .systemFont(ofSize: 12, weight: .semibold)
        hintTitle.textColor = NSColor(red: 0.592, green: 0.643, blue: 0.722, alpha: 1)
        hintBox.addSubview(hintTitle)

        let hintValue = NSTextField(labelWithString: "http://127.0.0.1:\(defaultPort)")
        hintValue.translatesAutoresizingMaskIntoConstraints = false
        hintValue.font = .monospacedSystemFont(ofSize: 14, weight: .medium)
        hintValue.textColor = NSColor(red: 0.89, green: 0.925, blue: 0.98, alpha: 1)
        hintBox.addSubview(hintValue)

        let label = NSTextField(labelWithString: tr("start.port"))
        label.translatesAutoresizingMaskIntoConstraints = false
        label.font = .systemFont(ofSize: 13, weight: .semibold)
        label.textColor = NSColor(red: 0.592, green: 0.643, blue: 0.722, alpha: 1)
        panel.addSubview(label)

        portField = NSTextField(string: defaultPort)
        portField.translatesAutoresizingMaskIntoConstraints = false
        portField.font = .monospacedDigitSystemFont(ofSize: 18, weight: .semibold)
        portField.alignment = .center
        portField.textColor = NSColor(red: 0.89, green: 0.925, blue: 0.98, alpha: 1)
        portField.isEditable = true
        portField.isSelectable = true
        portField.focusRingType = .none
        portField.isBezeled = true
        portField.bezelStyle = .roundedBezel
        portField.drawsBackground = true
        portField.backgroundColor = NSColor(red: 0.018, green: 0.027, blue: 0.043, alpha: 1)
        portField.wantsLayer = true
        portField.layer?.cornerRadius = 10
        portField.layer?.borderWidth = 1
        portField.layer?.borderColor = NSColor(red: 0.145, green: 0.188, blue: 0.267, alpha: 1).cgColor
        panel.addSubview(portField)

        startButton = NSButton(title: tr("start.button"), target: self, action: #selector(startClicked))
        startButton.translatesAutoresizingMaskIntoConstraints = false
        startButton.bezelStyle = .regularSquare
        startButton.isBordered = false
        startButton.controlSize = .large
        startButton.font = .systemFont(ofSize: 15, weight: .bold)
        startButton.keyEquivalent = "\r"
        startButton.wantsLayer = true
        startButton.layer?.backgroundColor = NSColor(red: 0.153, green: 0.82, blue: 0.498, alpha: 1).cgColor
        startButton.layer?.cornerRadius = 12
        startButton.contentTintColor = NSColor(red: 0.02, green: 0.07, blue: 0.045, alpha: 1)
        panel.addSubview(startButton)

        statusLabel = NSTextField(wrappingLabelWithString: "")
        statusLabel.translatesAutoresizingMaskIntoConstraints = false
        statusLabel.font = .systemFont(ofSize: 13)
        statusLabel.textColor = NSColor(red: 1.0, green: 0.361, blue: 0.451, alpha: 1)
        panel.addSubview(statusLabel)

        NSLayoutConstraint.activate([
            panel.centerXAnchor.constraint(equalTo: root.centerXAnchor),
            panel.centerYAnchor.constraint(equalTo: root.centerYAnchor, constant: 18),
            panel.widthAnchor.constraint(equalToConstant: 560),

            mark.topAnchor.constraint(equalTo: panel.topAnchor, constant: 34),
            mark.leadingAnchor.constraint(equalTo: panel.leadingAnchor, constant: 34),
            mark.widthAnchor.constraint(equalToConstant: 52),
            mark.heightAnchor.constraint(equalToConstant: 52),

            badge.centerYAnchor.constraint(equalTo: mark.centerYAnchor),
            badge.trailingAnchor.constraint(equalTo: panel.trailingAnchor, constant: -34),
            badge.widthAnchor.constraint(greaterThanOrEqualToConstant: 96),
            badge.heightAnchor.constraint(equalToConstant: 28),

            title.topAnchor.constraint(equalTo: mark.bottomAnchor, constant: 22),
            title.leadingAnchor.constraint(equalTo: panel.leadingAnchor, constant: 34),
            title.trailingAnchor.constraint(equalTo: panel.trailingAnchor, constant: -34),

            subtitle.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 6),
            subtitle.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            subtitle.trailingAnchor.constraint(equalTo: title.trailingAnchor),

            hintBox.topAnchor.constraint(equalTo: subtitle.bottomAnchor, constant: 22),
            hintBox.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            hintBox.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            hintBox.heightAnchor.constraint(equalToConstant: 64),

            hintTitle.leadingAnchor.constraint(equalTo: hintBox.leadingAnchor, constant: 16),
            hintTitle.centerYAnchor.constraint(equalTo: hintBox.centerYAnchor),
            hintTitle.widthAnchor.constraint(equalToConstant: 120),

            hintValue.leadingAnchor.constraint(equalTo: hintTitle.trailingAnchor, constant: 8),
            hintValue.trailingAnchor.constraint(equalTo: hintBox.trailingAnchor, constant: -16),
            hintValue.centerYAnchor.constraint(equalTo: hintBox.centerYAnchor),

            label.topAnchor.constraint(equalTo: hintBox.bottomAnchor, constant: 22),
            label.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            label.trailingAnchor.constraint(equalTo: title.trailingAnchor),

            portField.topAnchor.constraint(equalTo: label.bottomAnchor, constant: 8),
            portField.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            portField.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            portField.heightAnchor.constraint(equalToConstant: 52),

            startButton.topAnchor.constraint(equalTo: portField.bottomAnchor, constant: 16),
            startButton.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            startButton.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            startButton.heightAnchor.constraint(equalToConstant: 52),

            statusLabel.topAnchor.constraint(equalTo: startButton.bottomAnchor, constant: 12),
            statusLabel.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            statusLabel.trailingAnchor.constraint(equalTo: title.trailingAnchor),
            statusLabel.bottomAnchor.constraint(equalTo: panel.bottomAnchor, constant: -34)
        ])

        DispatchQueue.main.async { self.window.makeFirstResponder(self.portField) }
        return root
    }

    @objc private func startClicked() {
        let rawPort = portField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let port = rawPort.isEmpty ? defaultPort : rawPort

        guard let portValue = Int(port), (1...65535).contains(portValue) else {
            setStatus(tr("error.invalidPort"), isError: true)
            return
        }

        guard isPortAvailable(portValue) else {
            setStatus(String(format: tr("error.portInUse"), portValue), isError: true)
            return
        }

        startButton.isEnabled = false
        setStatus(tr("status.starting"), isError: false)

        do {
            try startBackend(port: port)
        } catch {
            startButton.isEnabled = true
            setStatus(String(format: tr("error.startFailed"), error.localizedDescription), isError: true)
            return
        }

        waitForServer(port: portValue) { ok in
            DispatchQueue.main.async {
                if ok {
                    self.loadDashboard(port: portValue)
                } else {
                    self.stopBackend(force: true)
                    self.startButton.isEnabled = true
                    self.setStatus(tr("error.startTimeout"), isError: true)
                }
            }
        }
    }

    private func startBackend(port: String) throws {
        stopBackend(force: true)

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
        window.toolbar = toolbar
        toolbar.isVisible = true
        window.title = "\(tr("app.name")) · \(port)"
    }

    @objc private func stopClicked() {
        stopBackend(force: true)
        webView.stopLoading()
        webView.loadHTMLString("", baseURL: nil)
        startButton.isEnabled = true
        statusLabel.stringValue = ""
        toolbar.isVisible = false
        window.toolbar = nil
        window.title = tr("app.name")
        window.contentView = startView
        DispatchQueue.main.async { self.window.makeFirstResponder(self.portField) }
    }

    private func stopBackend(force: Bool = false) {
        if let process = backendProcess, process.isRunning {
            process.terminate()
            if force {
                Thread.sleep(forTimeInterval: 0.35)
                if process.isRunning {
                    kill(process.processIdentifier, SIGKILL)
                }
            }
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
            return tr("error.missingResources")
        case .missingBackend:
            return tr("error.missingBackend")
        }
    }
}

extension LauncherAppDelegate {
    private static let stopItemIdentifier = NSToolbarItem.Identifier("stopGateway")
    private static let flexibleSpaceIdentifier = NSToolbarItem.Identifier.flexibleSpace

    func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        [Self.flexibleSpaceIdentifier, Self.stopItemIdentifier]
    }

    func toolbarDefaultItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        [Self.flexibleSpaceIdentifier, Self.stopItemIdentifier]
    }

    func toolbar(_ toolbar: NSToolbar, itemForItemIdentifier itemIdentifier: NSToolbarItem.Identifier, willBeInsertedIntoToolbar flag: Bool) -> NSToolbarItem? {
        guard itemIdentifier == Self.stopItemIdentifier else {
            return nil
        }

        let item = NSToolbarItem(itemIdentifier: itemIdentifier)
        let button = NSButton(title: tr("stop.button"), target: self, action: #selector(stopClicked))
        button.bezelStyle = .texturedRounded
        button.controlSize = .large
        button.image = NSImage(systemSymbolName: "stop.fill", accessibilityDescription: tr("stop.button"))
        button.imagePosition = .imageLeading
        button.font = .systemFont(ofSize: 13, weight: .semibold)
        item.label = tr("stop.button")
        item.paletteLabel = tr("stop.button")
        item.toolTip = tr("stop.tooltip")
        item.view = button
        return item
    }
}

let app = NSApplication.shared
let delegate = LauncherAppDelegate()
app.delegate = delegate
app.setActivationPolicy(.regular)
app.run()
