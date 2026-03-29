package template;

import net.kyori.adventure.text.Component;
import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.configuration.ClientSelectKnownPacksPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.play.ClientCommandChatPacket;
import net.minestom.server.network.packet.client.play.ClientConfigurationAckPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;
import net.minestom.server.network.packet.server.configuration.FinishConfigurationPacket;
import net.minestom.server.network.packet.server.configuration.SelectKnownPacksPacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.play.StartConfigurationPacket;
import net.minestom.server.network.packet.server.play.SystemChatPacket;
import proxy.ProxyNetworkContext;
import proxy.ProxyPackets;
import proxy.ProxySockets;
import proxy.via.InitialClientFrameReader;
import proxy.via.NativePacketCodec;
import proxy.via.ProxyViaBootstrap;
import proxy.via.ProxyViaSession;
import proxy.via.RawFrameContext;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Via proxy template.
 * <p>
 * Native protocol clients use the normal typed frontend path.
 * Non-native clients use raw client frames + Via translation on the frontend edge.
 */
public final class ViaProxyTemplate {
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("0.0.0.0", 25565);
    private static final InetSocketAddress DEFAULT_BACKEND = new InetSocketAddress("127.0.0.1", 25566);
    private static final int BACKEND_CONNECT_TIMEOUT_MS = 3000;

    private static final File VIA_DATA_FOLDER = new File("ViaProxy");

    /**
     * 1.20.2 introduced the login/config split.
     * Older clients need a small proxy-side bridge for initial login only.
     */
    private static final int CONFIG_PHASE_PROTOCOL = 764;

    public static void main(String[] args) throws Exception {
        MinecraftServer.init();
        ProxyViaBootstrap.init(VIA_DATA_FOLDER, MinecraftServer.PROTOCOL_VERSION);
        new ViaProxyTemplate().start();
    }

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final ServerSocketChannel server;

    public ViaProxyTemplate() throws IOException {
        this.server = ServerSocketChannel.open(StandardProtocolFamily.INET);
    }

    ClientPacket processClientPacket(Connection connection, ClientPacket packet) {
        if (packet instanceof ClientCommandChatPacket(String command)) {
            if (command.equalsIgnoreCase("server")) {
                connection.sendSystemMessage("Usage: /server host:port");
                return null;
            }

            if (command.regionMatches(true, 0, "server ", 0, 7)) {
                final String targetText = command.substring(7);
                final InetSocketAddress target = parseHostPort(targetText);

                if (target == null) {
                    connection.sendSystemMessage("Unknown target: " + targetText);
                } else {
                    connection.switchBackend(target);
                }
                return null;
            }
        }

        return packet;
    }

    ServerPacket processServerPacket(Connection connection, ServerPacket packet) {
        return packet;
    }

    public void start() throws Exception {
        server.bind(ADDRESS);
        System.out.println("Via proxy started on: " + ADDRESS + ", default backend: " + DEFAULT_BACKEND);

        Thread.startVirtualThread(this::listenCommands);
        Thread.startVirtualThread(this::listenConnections);

        stopLatch.await();
        ProxySockets.closeQuietly(server);
        System.out.println("Via proxy stopped");
    }

    private void listenCommands() {
        Scanner scanner = new Scanner(System.in);
        while (serverRunning()) {
            if (!scanner.hasNextLine()) break;
            final String line = scanner.nextLine();
            if (line.equalsIgnoreCase("stop")) {
                stop();
            } else if (line.equalsIgnoreCase("gc")) {
                System.gc();
            }
        }
    }

    private void listenConnections() {
        while (serverRunning()) {
            SocketChannel client = null;
            try {
                client = server.accept();
                if (client == null) continue;

                final SocketChannel acceptedClient = client;
                client = null;

                Thread.startVirtualThread(() -> {
                    try {
                        Connection connection = new Connection(acceptedClient);
                        connection.start();
                    } catch (IOException e) {
                        ProxySockets.closeQuietly(acceptedClient);
                        if (serverRunning()) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (IOException e) {
                ProxySockets.closeQuietly(client);
                if (serverRunning()) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void stop() {
        if (!stop.compareAndSet(false, true)) return;
        ProxySockets.closeQuietly(server);
        stopLatch.countDown();
    }

    private boolean serverRunning() {
        return !stop.get();
    }

    private static InetSocketAddress parseHostPort(String value) {
        final int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) return null;

        final String host = value.substring(0, separator);
        final String portText = value.substring(separator + 1);
        try {
            final int port = Integer.parseInt(portText);
            return new InetSocketAddress(host, port);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static byte[] frameClientPacket(ConnectionState state, ClientPacket packet) {
        final NetworkBuffer buffer = NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.process());
        PacketWriting.writeFramedPacket(buffer, state, packet, 0);

        byte[] bytes = new byte[(int) buffer.writeIndex()];
        buffer.copyTo(0, bytes, 0, bytes.length);
        return bytes;
    }

    enum FrontendMode {
        NATIVE,
        VIA
    }

    enum SwitchState {
        NONE,
        WAITING_PENDING_LOGIN_SUCCESS,
        WAITING_CLIENT_CONFIGURATION_ACK,
        IN_CONFIGURATION
    }

    record HandshakeInfo(int protocolVersion, String host, int port, ClientHandshakePacket.Intent intent) {
    }

    static final class BackendConnection {
        final InetSocketAddress address;
        final SocketChannel channel;
        final ProxyNetworkContext context = new ProxyNetworkContext();
        final AtomicBoolean closed = new AtomicBoolean(false);

        BackendConnection(InetSocketAddress address, SocketChannel channel) {
            this.address = address;
            this.channel = channel;
        }
    }

    final class Connection {
        private final String remoteAddress;
        private final SocketChannel client;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final ProxyNetworkContext nativeClientContext = new ProxyNetworkContext();
        private final RawFrameContext viaClientFrames = new RawFrameContext();

        /**
         * Tiny in-memory switch/routing state only.
         * No socket connect/read/write happens while holding it.
         */
        private final Object stateLock = new Object();

        private FrontendMode frontendMode;
        private ProxyViaSession viaSession;
        private ConnectionState viaClientReadState = ConnectionState.HANDSHAKE;
        private ConnectionState viaClientWriteState = ConnectionState.HANDSHAKE;

        private BackendConnection activeBackend;
        private BackendConnection pendingBackend;
        private SwitchState switchState = SwitchState.NONE;


        private final ArrayDeque<ClientPacket> queuedClientPackets = new ArrayDeque<>();
        private final ArrayDeque<ServerPacket> pendingConfigurationPackets = new ArrayDeque<>();

        private int clientProtocolVersion = MinecraftServer.PROTOCOL_VERSION;
        private String requestedHost = "localhost";
        private int requestedPort = 25565;

        private String username;
        private UUID profileId;
        private UUID gameProfileId;
        private String gameProfileName;

        Connection(SocketChannel client) throws IOException {
            this.client = client;
            ProxySockets.configure(client);
            this.remoteAddress = String.valueOf(client.getRemoteAddress());

            BackendConnection initial = connectBackend(DEFAULT_BACKEND);
            synchronized (stateLock) {
                this.activeBackend = initial;
            }

            if (initial != null) {
                System.out.println("Accepted " + remoteAddress + ", connected backend " + initial.address);
            } else {
                System.out.println("Accepted " + remoteAddress + ", backend unavailable " + DEFAULT_BACKEND);
            }
        }

        void start() {
            Thread.startVirtualThread(this::runConnection);
        }

        private void runConnection() {
            try {
                if (!bootstrapFrontendMode()) {
                    close();
                    return;
                }

                final BackendConnection active;
                synchronized (stateLock) {
                    active = activeBackend;
                }
                if (active != null) {
                    startBackendLoops(active);
                }

                switch (frontendMode) {
                    case NATIVE -> {
                        Thread.startVirtualThread(this::nativeClientWriteLoop);
                        nativeClientReadLoop();
                    }
                    case VIA -> {
                        Thread.startVirtualThread(this::viaClientWriteLoop);
                        viaClientReadLoop();
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
                close();
            }
        }

        private boolean bootstrapFrontendMode() throws IOException {
            final InitialClientFrameReader bootstrapReader = new InitialClientFrameReader();
            final InitialClientFrameReader.Result bootstrap = bootstrapReader.readFirstFrame(client);

            final HandshakeInfo handshake = sniffHandshake(bootstrap.firstBody());
            if (handshake == null) {
                System.out.println("Failed to sniff handshake from " + remoteAddress);
                return false;
            }

            this.clientProtocolVersion = handshake.protocolVersion();
            this.requestedHost = handshake.host();
            this.requestedPort = handshake.port();

            final ClientHandshakePacket nativeHandshake = new ClientHandshakePacket(
                    MinecraftServer.PROTOCOL_VERSION,
                    handshake.host(),
                    handshake.port(),
                    handshake.intent()
            );

            final ClientPacket forwardedHandshake = ViaProxyTemplate.this.processClientPacket(this, nativeHandshake);
            if (forwardedHandshake == null) {
                return false;
            }
            cacheClientMetadata(forwardedHandshake);

            if (handshake.protocolVersion() == MinecraftServer.PROTOCOL_VERSION) {
                this.frontendMode = FrontendMode.NATIVE;

                final byte[] nativeHandshakeFrame = frameClientPacket(ConnectionState.HANDSHAKE, forwardedHandshake);

                nativeClientContext.feedRawBytes(nativeHandshakeFrame);
                nativeClientContext.feedRawBytes(bootstrap.remainingRaw());
                nativeClientContext.consumeBufferedClients(this::handleNativeClientPacket);

                System.out.println("Using native frontend path for " + remoteAddress + " (protocol " + handshake.protocolVersion() + ")");
                return true;
            }

            this.frontendMode = FrontendMode.VIA;

            final ConnectionState initialViaState =
                    PacketVanilla.nextClientState(forwardedHandshake, ConnectionState.HANDSHAKE);

            this.viaSession = new ProxyViaSession(
                    handshake.protocolVersion(),
                    MinecraftServer.PROTOCOL_VERSION,
                    initialViaState
            );

            if (!viaSession.supported()) {
                System.out.println("Unsupported translated path for " + remoteAddress + " (client protocol " + handshake.protocolVersion() + ")");
                return false;
            }

            viaClientReadState = initialViaState;
            viaClientWriteState = initialViaState;

            final BackendConnection current;
            synchronized (stateLock) {
                current = activeBackend;
            }
            if (current != null) {
                current.context.writeClient(forwardedHandshake);
                ProxyPackets.syncAfterClientForward(forwardedHandshake, current.context);
            }

            viaClientFrames.feedRawBytes(bootstrap.remainingRaw());
            if (bootstrap.remainingRaw().length > 0) {
                viaClientFrames.consumeBufferedFrames(this::handleViaClientBody);
            }

            System.out.println("Using Via frontend path for " + remoteAddress + " (client protocol " + handshake.protocolVersion() + " -> native " + MinecraftServer.PROTOCOL_VERSION + ")");
            return true;
        }

        void sendSystemMessage(String message) {
            if (closed.get()) return;

            if (frontendMode == FrontendMode.NATIVE) {
                if (nativeClientContext.writeState() != ConnectionState.PLAY) return;
                sendToClient(new SystemChatPacket(Component.text(message), false));
                return;
            }

            if (viaClientWriteState != ConnectionState.PLAY) return;
            sendToClient(new SystemChatPacket(Component.text(message), false));
        }

        void disconnect(String message) {
            if (closed.get()) return;

            if (frontendMode == FrontendMode.NATIVE) {
                final ConnectionState state = nativeClientContext.writeState();
                if (state == ConnectionState.LOGIN ||
                        state == ConnectionState.CONFIGURATION ||
                        state == ConnectionState.PLAY) {
                    nativeClientContext.writeServer(ProxyPackets.disconnectPacket(state, Component.text(message)));
                    closeSoon();
                } else {
                    close();
                }
                return;
            }

            final ConnectionState state = viaClientWriteState;
            if (state == ConnectionState.LOGIN ||
                    state == ConnectionState.CONFIGURATION ||
                    state == ConnectionState.PLAY) {
                sendToClient(ProxyPackets.disconnectPacket(state, Component.text(message)));
                closeSoon();
            } else {
                close();
            }
        }

        boolean switchBackend(InetSocketAddress address) {
            if (address == null || closed.get()) return false;

            final BackendConnection currentActive;
            synchronized (stateLock) {
                if (frontendMode == FrontendMode.VIA && clientProtocolVersion < CONFIG_PHASE_PROTOCOL) {
                    sendSystemMessage("Switching is not supported for this translated client version.");
                    return false;
                }

                final ConnectionState frontendReadState = switch (frontendMode) {
                    case NATIVE -> nativeClientContext.readState();
                    case VIA -> viaClientReadState;
                };

                if (frontendReadState != ConnectionState.PLAY) {
                    sendSystemMessage("You must be fully in-game to switch.");
                    return false;
                }

                if (switchState != SwitchState.NONE) {
                    sendSystemMessage("Already switching.");
                    return false;
                }

                currentActive = activeBackend;
                if (currentActive == null) {
                    sendSystemMessage("No backend connected.");
                    return false;
                }

                if (currentActive.address.equals(address)) {
                    sendSystemMessage("Already connected to " + address + ".");
                    return false;
                }
            }

            final BackendConnection next = connectBackend(address);
            if (next == null) {
                sendSystemMessage("Backend " + address + " is offline.");
                return false;
            }

            synchronized (stateLock) {
                if (closed.get() || switchState != SwitchState.NONE || activeBackend != currentActive) {
                    silentCloseBackend(next);
                    return false;
                }

                pendingBackend = next;
                queuedClientPackets.clear();
                pendingConfigurationPackets.clear();
                switchState = SwitchState.WAITING_PENDING_LOGIN_SUCCESS;
            }

            startBackendLoops(next);
            bootstrapPendingBackend(next);

            sendSystemMessage("Connecting to " + address + "...");
            return true;
        }

        private void clientReadLoop() {
            while (!closed.get()) {
                boolean ok = nativeClientContext.readClient(buffer -> {
                    try {
                        buffer.readChannel(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleNativeClientPacket);

                if (!ok) {
                    close();
                    return;
                }
            }
        }

        private void nativeClientReadLoop() {
            clientReadLoop();
        }

        private void nativeClientWriteLoop() {
            while (!closed.get()) {
                boolean ok = nativeClientContext.flush(buffer -> {
                    try {
                        buffer.writeChannel(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (!ok) {
                    close();
                    return;
                }
            }
        }

        private void viaClientReadLoop() {
            while (!closed.get()) {
                boolean ok = viaClientFrames.readFrames(buffer -> {
                    try {
                        buffer.readChannel(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleViaClientBody);

                if (!ok) {
                    close();
                    return;
                }
            }
        }

        private void viaClientWriteLoop() {
            while (!closed.get()) {
                boolean ok = viaClientFrames.flush(buffer -> {
                    try {
                        buffer.writeChannel(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (!ok) {
                    close();
                    return;
                }
            }
        }

        private void startBackendLoops(BackendConnection backend) {
            Thread.startVirtualThread(() -> backendReadLoop(backend));
            Thread.startVirtualThread(() -> backendWriteLoop(backend));
        }

        private void backendReadLoop(BackendConnection backend) {
            while (!closed.get() && !backend.closed.get()) {
                boolean ok = backend.context.readServer(buffer -> {
                    try {
                        buffer.readChannel(backend.channel);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, packet -> handleBackendPacket(backend, packet));

                if (!ok) {
                    onBackendClosed(backend);
                    return;
                }
            }
        }

        private void backendWriteLoop(BackendConnection backend) {
            while (!closed.get() && !backend.closed.get()) {
                boolean ok = backend.context.flush(buffer -> {
                    try {
                        buffer.writeChannel(backend.channel);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (!ok) {
                    onBackendClosed(backend);
                    return;
                }
            }
        }

        private void handleNativeClientPacket(ClientPacket rawPacket) {
            final ClientPacket packet = ViaProxyTemplate.this.processClientPacket(this, rawPacket);
            if (packet == null) return;
            cacheClientMetadata(packet);

            routeFrontendClientPacket(packet);
        }

        private void handleViaClientBody(byte[] body) {
            final ProxyViaSession.TranslationResult result;
            try {
                result = viaSession.translateServerbound(body);
            } catch (Exception e) {
                e.printStackTrace();
                disconnect("Protocol translation failed.");
                return;
            }

            for (byte[] injectedBody : result.injectedBodies()) {
                handleTranslatedViaServerboundBody(injectedBody, true);
            }

            final byte[] nativeBody = result.body();
            if (nativeBody == null) {
                return;
            }

            handleTranslatedViaServerboundBody(nativeBody, false);
        }

        private void handleTranslatedViaServerboundBody(byte[] nativeBody, boolean injected) {
            final ClientPacket rawPacket;
            try {
                rawPacket = NativePacketCodec.readClient(viaClientReadState, nativeBody);
            } catch (RuntimeException e) {
                if (injected && dropLateInjectedTransitionPacket(nativeBody)) {
                    return;
                }
                throw e;
            }

            final ConnectionState previousReadState = viaClientReadState;
            final ConnectionState nextReadState = PacketVanilla.nextClientState(rawPacket, previousReadState);

            viaClientReadState = nextReadState;
            if (viaSession != null && nextReadState != previousReadState) {
                viaSession.setState(nextReadState);
            }

            final ClientPacket packet = ViaProxyTemplate.this.processClientPacket(this, rawPacket);
            if (packet == null) return;
            cacheClientMetadata(packet);

            routeFrontendClientPacket(packet);
        }

        private void routeFrontendClientPacket(ClientPacket packet) {
            synchronized (stateLock) {
                if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK ||
                        switchState == SwitchState.IN_CONFIGURATION) {
                    handleSwitchingClientPacketLocked(packet);
                    return;
                }

                final BackendConnection current = activeBackend;
                if (current == null) {
                    handleOfflineClientPacket(packet);
                    return;
                }

                current.context.writeClient(packet);

                if (frontendMode == FrontendMode.NATIVE) {
                    ProxyPackets.syncAfterClientForward(packet, nativeClientContext, current.context);
                } else {
                    ProxyPackets.syncAfterClientForward(packet, current.context);
                }
            }
        }

        private void handleSwitchingClientPacketLocked(ClientPacket packet) {
            if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK) {
                if (packet instanceof ClientConfigurationAckPacket) {
                    while (!pendingConfigurationPackets.isEmpty()) {
                        sendToClient(pendingConfigurationPackets.removeFirst());
                    }
                    switchState = SwitchState.IN_CONFIGURATION;
                    return;
                }

                queuedClientPackets.addLast(packet);
                return;
            }

            if (switchState == SwitchState.IN_CONFIGURATION) {
                final BackendConnection next = pendingBackend;
                if (next == null) return;

                next.context.writeClient(packet);
                ProxyPackets.syncAfterClientForward(packet, next.context);

                if (packet instanceof ClientFinishConfigurationPacket) {
                    completeSwitchLocked();
                }
            }
        }

        private void completeSwitchLocked() {
            final BackendConnection old = activeBackend;
            final BackendConnection next = pendingBackend;
            if (next == null) return;

            activeBackend = next;
            pendingBackend = null;
            switchState = SwitchState.NONE;
            pendingConfigurationPackets.clear();

            while (!queuedClientPackets.isEmpty()) {
                final ClientPacket queued = queuedClientPackets.removeFirst();
                next.context.writeClient(queued);
                ProxyPackets.syncAfterClientForward(queued, next.context);
            }

            if (old != null && old != next) {
                silentCloseBackend(old);
            }

            sendSystemMessage("Connected to " + next.address + ".");
        }

        private void handleBackendPacket(BackendConnection backend, ServerPacket rawPacket) {
            synchronized (stateLock) {
                if (backend == activeBackend) {
                    if (switchState == SwitchState.NONE ||
                            switchState == SwitchState.WAITING_PENDING_LOGIN_SUCCESS) {
                        handleActiveBackendPacketLocked(rawPacket);
                    }
                    return;
                }

                if (backend != pendingBackend) {
                    return;
                }

                handlePendingBackendPacketLocked(backend, rawPacket);
            }
        }

        private void handleActiveBackendPacketLocked(ServerPacket rawPacket) {
            if (rawPacket instanceof LoginSuccessPacket loginSuccessPacket) {
                gameProfileId = loginSuccessPacket.gameProfile().uuid();
                gameProfileName = loginSuccessPacket.gameProfile().name();

                if (viaSession != null) {
                    viaSession.onLoginSuccess(gameProfileName, gameProfileId);
                }
            }

            final boolean bridgeLoginAck = rawPacket instanceof LoginSuccessPacket && legacyConfigBridge();
            final boolean bridgeFinishConfig = rawPacket instanceof FinishConfigurationPacket && legacyConfigBridge();

            if (legacyConfigBridge() && rawPacket instanceof SelectKnownPacksPacket selectKnownPacksPacket) {
                bridgeLegacyKnownPacks();
                return;
            }

            final ServerPacket packet = ViaProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            sendToClient(packet);

            if (bridgeLoginAck) {
                bridgeLegacyLoginAcknowledged();
            }
            if (bridgeFinishConfig) {
                bridgeLegacyFinishConfiguration();
            }
        }

        private void handlePendingBackendPacketLocked(BackendConnection backend, ServerPacket rawPacket) {
            if (switchState == SwitchState.WAITING_PENDING_LOGIN_SUCCESS) {
                if (rawPacket instanceof LoginSuccessPacket) {
                    final ClientLoginAcknowledgedPacket loginAck = new ClientLoginAcknowledgedPacket();
                    backend.context.writeClient(loginAck);
                    ProxyPackets.syncAfterClientForward(loginAck, backend.context);

                    beginFrontendConfigurationLocked();
                    switchState = SwitchState.WAITING_CLIENT_CONFIGURATION_ACK;
                }
                return;
            }

            final ServerPacket packet = ViaProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK) {
                pendingConfigurationPackets.addLast(packet);
                return;
            }

            if (switchState == SwitchState.IN_CONFIGURATION) {
                sendToClient(packet);
            }
        }

        private void beginFrontendConfigurationLocked() {
            /*
             * Only the frontend WRITE side moves here.
             * The frontend READ side moves when the client actually sends ClientConfigurationAckPacket.
             */
            sendToClient(new StartConfigurationPacket());
        }

        private boolean dropLateInjectedTransitionPacket(byte[] nativeBody) {
            if (viaClientReadState != ConnectionState.PLAY) {
                return false;
            }

            final ClientPacket configPacket = tryReadClientPacket(ConnectionState.CONFIGURATION, nativeBody);
            if (configPacket != null) {
                return true;
            }

            final ClientPacket loginPacket = tryReadClientPacket(ConnectionState.LOGIN, nativeBody);
            return loginPacket != null;
        }

        private ClientPacket tryReadClientPacket(ConnectionState state, byte[] body) {
            try {
                return NativePacketCodec.readClient(state, body);
            } catch (RuntimeException e) {
                return null;
            }
        }

        private void sendToClient(ServerPacket packet) {
            if (packet == null || closed.get()) return;

            if (frontendMode == FrontendMode.NATIVE) {
                nativeClientContext.writeServer(packet);
                ProxyPackets.syncAfterServerForward(packet, nativeClientContext);
                return;
            }

            final ConnectionState beforeWriteState = viaClientWriteState;
            final ConnectionState nextWriteState = PacketVanilla.nextServerState(packet, viaClientWriteState);

            final byte[] nativeBody = NativePacketCodec.writeServerBody(beforeWriteState, packet);

            final ProxyViaSession.TranslationResult result;
            try {
                result = viaSession.translateClientbound(nativeBody);
            } catch (Exception e) {
                e.printStackTrace();
                disconnect("Protocol translation failed.");
                return;
            }

            for (byte[] injectedBody : result.injectedBodies()) {
                viaClientFrames.writeBody(injectedBody);
            }

            final byte[] clientBody = result.body();
            if (clientBody == null) {
                viaClientWriteState = nextWriteState;
                if (viaSession != null && shouldAdvanceViaStateAfterClientbound(packet, nextWriteState)) {
                    viaSession.setState(nextWriteState);
                }
                return;
            }

            viaClientFrames.writeBody(clientBody);

            viaClientWriteState = nextWriteState;
            if (viaSession != null && shouldAdvanceViaStateAfterClientbound(packet, nextWriteState)) {
                viaSession.setState(nextWriteState);
            }
        }

        private boolean shouldAdvanceViaStateAfterClientbound(ServerPacket packet, ConnectionState nextWriteState) {
            if (packet instanceof LoginSuccessPacket && nextWriteState == ConnectionState.CONFIGURATION) {
                return false;
            }
            if (packet instanceof StartConfigurationPacket && nextWriteState == ConnectionState.CONFIGURATION) {
                return false;
            }
            return !(packet instanceof FinishConfigurationPacket && nextWriteState == ConnectionState.PLAY);
        }

        private void handleOfflineClientPacket(ClientPacket packet) {
            if (packet instanceof StatusRequestPacket) {
                sendToClient(ProxyPackets.offlineResponse("Backend is offline"));
                return;
            }
            if (packet instanceof ClientPingRequestPacket pingRequestPacket) {
                sendToClient(new PingResponsePacket(pingRequestPacket.number()));
                closeSoon();
                return;
            }
            if (packet instanceof ClientLoginStartPacket) {
                disconnect("Backend is offline.");
            }
        }

        private HandshakeInfo sniffHandshake(byte[] bodyBytes) {
            try {
                final NetworkBuffer body = NetworkBuffer.wrap(bodyBytes, 0, bodyBytes.length, MinecraftServer.process());

                final int packetId = body.read(NetworkBuffer.VAR_INT);
                if (packetId != 0) return null;

                final int protocolVersion = body.read(NetworkBuffer.VAR_INT);
                final String host = body.read(NetworkBuffer.STRING);
                final int port = body.read(NetworkBuffer.UNSIGNED_SHORT);
                final int intentId = body.read(NetworkBuffer.VAR_INT);

                final ClientHandshakePacket.Intent intent = switch (intentId) {
                    case 1 -> ClientHandshakePacket.Intent.STATUS;
                    case 2 -> ClientHandshakePacket.Intent.LOGIN;
                    case 3 -> ClientHandshakePacket.Intent.TRANSFER;
                    default -> throw new IllegalStateException("Unknown handshake intent: " + intentId);
                };

                return new HandshakeInfo(protocolVersion, host, port, intent);
            } catch (Exception e) {
                return null;
            }
        }

        private void cacheClientMetadata(ClientPacket packet) {
            if (packet instanceof ClientHandshakePacket handshakePacket) {
                requestedHost = handshakePacket.serverAddress();
                requestedPort = handshakePacket.serverPort();
                return;
            }

            if (packet instanceof ClientLoginStartPacket loginStartPacket) {
                username = loginStartPacket.username();
                profileId = loginStartPacket.profileId();
            }
        }

        private boolean legacyConfigBridge() {
            return frontendMode == FrontendMode.VIA && clientProtocolVersion < CONFIG_PHASE_PROTOCOL;
        }

        private void bridgeLegacyLoginAcknowledged() {
            final BackendConnection current = activeBackend;
            if (current == null) return;

            final ClientLoginAcknowledgedPacket packet = new ClientLoginAcknowledgedPacket();
            current.context.writeClient(packet);
            ProxyPackets.syncAfterClientForward(packet, current.context);

            viaClientReadState = ConnectionState.CONFIGURATION;
            if (viaSession != null) {
                viaSession.setState(ConnectionState.CONFIGURATION);
            }
        }

        private void bridgeLegacyFinishConfiguration() {
            final BackendConnection current = activeBackend;
            if (current == null) return;

            final ClientFinishConfigurationPacket packet = new ClientFinishConfigurationPacket();
            current.context.writeClient(packet);
            ProxyPackets.syncAfterClientForward(packet, current.context);

            viaClientReadState = ConnectionState.PLAY;
            if (viaSession != null) {
                viaSession.setState(ConnectionState.PLAY);
            }
        }

        private void bridgeLegacyKnownPacks() {
            final BackendConnection current = activeBackend;
            if (current == null) return;
            final ClientSelectKnownPacksPacket packet =
                    new ClientSelectKnownPacksPacket(List.of());

            current.context.writeClient(packet);
            ProxyPackets.syncAfterClientForward(packet, current.context);
        }

        private void bootstrapPendingBackend(BackendConnection backend) {
            if (username == null) {
                throw new IllegalStateException("Cannot switch backend before login data was cached");
            }

            final ClientHandshakePacket handshakePacket = new ClientHandshakePacket(
                    MinecraftServer.PROTOCOL_VERSION,
                    requestedHost,
                    requestedPort,
                    ClientHandshakePacket.Intent.LOGIN
            );
            backend.context.writeClient(handshakePacket);
            ProxyPackets.syncAfterClientForward(handshakePacket, backend.context);

            final ClientLoginStartPacket loginStartPacket = new ClientLoginStartPacket(username, profileId);
            backend.context.writeClient(loginStartPacket);
        }

        private BackendConnection connectBackend(InetSocketAddress address) {
            try {
                SocketChannel channel = ProxySockets.connect(address, BACKEND_CONNECT_TIMEOUT_MS);
                return new BackendConnection(address, channel);
            } catch (IOException e) {
                return null;
            }
        }

        private void onBackendClosed(BackendConnection backend) {
            if (closed.get()) return;
            if (!backend.closed.compareAndSet(false, true)) return;

            backend.context.close();
            ProxySockets.closeQuietly(backend.channel);

            String disconnectMessage = null;
            String systemMessage = null;

            synchronized (stateLock) {
                if (backend == pendingBackend) {
                    pendingBackend = null;
                    pendingConfigurationPackets.clear();
                    queuedClientPackets.clear();

                    if (switchState == SwitchState.WAITING_PENDING_LOGIN_SUCCESS) {
                        /*
                         * Client still in PLAY. Safe to abort and stay on old backend.
                         */
                        switchState = SwitchState.NONE;
                        systemMessage = "Backend " + backend.address + " is offline.";
                    } else {
                        /*
                         * Client already entered configuration for the switch.
                         * This template does not try to roll back that state.
                         */
                        switchState = SwitchState.NONE;
                        disconnectMessage = "Switch to " + backend.address + " failed while reconfiguring.";
                    }
                } else if (backend == activeBackend) {
                    if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK ||
                            switchState == SwitchState.IN_CONFIGURATION) {
                        activeBackend = null;
                    } else {
                        activeBackend = null;
                        disconnectMessage = "Backend " + backend.address + " closed.";
                    }
                }
            }

            if (disconnectMessage != null) {
                disconnect(disconnectMessage);
                return;
            }
            if (systemMessage != null) {
                sendSystemMessage(systemMessage);
            }
        }

        private void silentCloseBackend(BackendConnection backend) {
            if (backend == null) return;
            if (!backend.closed.compareAndSet(false, true)) return;

            backend.context.close();
            ProxySockets.closeQuietly(backend.channel);
        }

        private void closeSoon() {
            Thread.startVirtualThread(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                close();
            });
        }

        private void close() {
            if (!closed.compareAndSet(false, true)) return;

            nativeClientContext.close();
            viaClientFrames.close();
            if (viaSession != null) viaSession.close();

            final BackendConnection active;
            final BackendConnection pending;
            synchronized (stateLock) {
                active = activeBackend;
                pending = pendingBackend;

                activeBackend = null;
                pendingBackend = null;
                switchState = SwitchState.NONE;

                pendingConfigurationPackets.clear();
                queuedClientPackets.clear();
            }

            silentCloseBackend(active);
            if (pending != active) {
                silentCloseBackend(pending);
            }

            ProxySockets.closeQuietly(client);
        }
    }
}