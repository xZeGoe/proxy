package template;

import net.kyori.adventure.text.Component;
import net.minestom.server.MinecraftServer;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.play.ClientCommandChatPacket;
import net.minestom.server.network.packet.client.play.ClientConfigurationAckPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.play.StartConfigurationPacket;
import net.minestom.server.network.packet.server.play.SystemChatPacket;
import proxy.ProxyNetworkContext;
import proxy.ProxyPackets;
import proxy.ProxySockets;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Switch proxy template.
 * <p>
 * Assumes:
 * - all backends use the same native protocol version
 * - all backends share the same registries
 */
public final class SwitchProxyTemplate {
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("0.0.0.0", 25565);
    private static final InetSocketAddress LIMBO_BACKEND = new InetSocketAddress("127.0.0.1", 25566);
    private static final InetSocketAddress SURVIVAL_BACKEND = new InetSocketAddress("127.0.0.1", 25567);
    private static final int BACKEND_CONNECT_TIMEOUT_MS = 3000;

    public static void main(String[] args) throws Exception {
        MinecraftServer.init();
        new SwitchProxyTemplate().start();
    }

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final ServerSocketChannel server;

    public SwitchProxyTemplate() throws IOException {
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
        System.out.println("Switch proxy started on: " + ADDRESS + ", default backend: " + LIMBO_BACKEND);

        Thread.startVirtualThread(this::listenCommands);
        Thread.startVirtualThread(this::listenConnections);

        stopLatch.await();
        ProxySockets.closeQuietly(server);
        System.out.println("Switch proxy stopped");
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

    enum SwitchState {
        NONE,
        WAITING_PENDING_LOGIN_SUCCESS,
        WAITING_CLIENT_CONFIGURATION_ACK,
        IN_CONFIGURATION
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
        private final ProxyNetworkContext clientContext = new ProxyNetworkContext();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        /**
         * Tiny in-memory switch/routing state only.
         * No socket connect/read/write happens while holding it.
         */
        private final Object stateLock = new Object();

        private BackendConnection activeBackend;
        private BackendConnection pendingBackend;
        private SwitchState switchState = SwitchState.NONE;

        private final ArrayDeque<ClientPacket> queuedClientPackets = new ArrayDeque<>();
        private final ArrayDeque<ServerPacket> pendingConfigurationPackets = new ArrayDeque<>();

        private int protocolVersion = MinecraftServer.PROTOCOL_VERSION;
        private String requestedHost = "localhost";
        private int requestedPort = 25565;

        private String username;
        private UUID profileId;

        Connection(SocketChannel client) throws IOException {
            this.client = client;
            ProxySockets.configure(client);
            this.remoteAddress = String.valueOf(client.getRemoteAddress());

            BackendConnection initial = connectBackend(LIMBO_BACKEND);
            synchronized (stateLock) {
                this.activeBackend = initial;
            }

            if (initial != null) {
                System.out.println("Accepted " + remoteAddress + ", connected backend " + initial.address);
            } else {
                System.out.println("Accepted " + remoteAddress + ", backend unavailable " + LIMBO_BACKEND);
            }
        }

        void start() {
            Thread.startVirtualThread(this::clientReadLoop);
            Thread.startVirtualThread(this::clientWriteLoop);

            final BackendConnection active;
            synchronized (stateLock) {
                active = activeBackend;
            }
            if (active != null) {
                startBackendLoops(active);
            }
        }

        void sendSystemMessage(String message) {
            if (closed.get()) return;
            if (clientContext.writeState() != ConnectionState.PLAY) return;
            sendToClient(new SystemChatPacket(Component.text(message), false));
        }

        void disconnect(String message) {
            if (closed.get()) return;

            final ConnectionState state = clientContext.writeState();
            if (state == ConnectionState.LOGIN ||
                    state == ConnectionState.CONFIGURATION ||
                    state == ConnectionState.PLAY) {
                clientContext.writeServer(ProxyPackets.disconnectPacket(state, Component.text(message)));
                closeSoon();
            } else {
                close();
            }
        }

        boolean switchBackend(InetSocketAddress address) {
            if (address == null || closed.get()) return false;

            final BackendConnection currentActive;
            synchronized (stateLock) {
                if (clientContext.readState() != ConnectionState.PLAY) {
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
                boolean ok = clientContext.readClient(buffer -> {
                    try {
                        buffer.readChannel(client);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleClientPacket);

                if (!ok) {
                    close();
                    return;
                }
            }
        }

        private void clientWriteLoop() {
            while (!closed.get()) {
                boolean ok = clientContext.flush(buffer -> {
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

        private void handleClientPacket(ClientPacket rawPacket) {
            final ClientPacket packet = SwitchProxyTemplate.this.processClientPacket(this, rawPacket);
            if (packet == null) return;
            cacheClientMetadata(packet);


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
                ProxyPackets.syncAfterClientForward(packet, clientContext, current.context);
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
            final ServerPacket packet = SwitchProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            sendToClient(packet);
        }

        private void handlePendingBackendPacketLocked(BackendConnection backend, ServerPacket rawPacket) {
            if (switchState == SwitchState.WAITING_PENDING_LOGIN_SUCCESS) {
                if (rawPacket instanceof LoginSuccessPacket) {
                    final ClientLoginAcknowledgedPacket loginAck = new ClientLoginAcknowledgedPacket();
                    backend.context.writeClient(loginAck);
                    ProxyPackets.syncAfterClientForward(loginAck, backend.context);

                    /*
                     * Send the client into configuration.
                     * IMPORTANT:
                     * only the frontend WRITE state advances here.
                     * The frontend READ state must stay in PLAY until the client actually sends
                     * ClientConfigurationAckPacket, which PacketReading/PacketVanilla will transition automatically.
                     */
                    sendToClient(new StartConfigurationPacket());

                    switchState = SwitchState.WAITING_CLIENT_CONFIGURATION_ACK;
                }
                return;
            }

            final ServerPacket packet = SwitchProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK) {
                pendingConfigurationPackets.addLast(packet);
                return;
            }

            if (switchState == SwitchState.IN_CONFIGURATION) {
                sendToClient(packet);
            }
        }

        private void sendToClient(ServerPacket packet) {
            if (packet == null || closed.get()) return;

            clientContext.writeServer(packet);
            ProxyPackets.syncAfterServerForward(packet, clientContext);
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

        private void cacheClientMetadata(ClientPacket packet) {
            if (packet instanceof ClientHandshakePacket handshakePacket) {
                protocolVersion = handshakePacket.protocolVersion();
                requestedHost = handshakePacket.serverAddress();
                requestedPort = handshakePacket.serverPort();
                return;
            }

            if (packet instanceof ClientLoginStartPacket loginStartPacket) {
                username = loginStartPacket.username();
                profileId = loginStartPacket.profileId();
            }
        }

        private void bootstrapPendingBackend(BackendConnection backend) {
            if (username == null) {
                throw new IllegalStateException("Cannot switch backend before login data was cached");
            }

            final ClientHandshakePacket handshakePacket = new ClientHandshakePacket(
                    protocolVersion,
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
                        switchState = SwitchState.NONE;
                        systemMessage = "Backend " + backend.address + " is offline.";
                    } else {
                        switchState = SwitchState.NONE;
                        disconnectMessage = "Switch to " + backend.address + " failed while reconfiguring.";
                    }
                } else if (backend == activeBackend) {
                    if (switchState == SwitchState.WAITING_CLIENT_CONFIGURATION_ACK || switchState == SwitchState.IN_CONFIGURATION) {
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

            clientContext.close();

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