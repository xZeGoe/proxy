package template;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import net.kyori.adventure.text.Component;
import net.minestom.server.MinecraftServer;
import net.minestom.server.coordinate.CoordConversion;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.configuration.ClientSelectKnownPacksPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.play.ClientCommandChatPacket;
import net.minestom.server.network.packet.client.play.ClientInteractEntityPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;
import net.minestom.server.network.packet.server.configuration.FinishConfigurationPacket;
import net.minestom.server.network.packet.server.configuration.SelectKnownPacksPacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.play.ChunkDataPacket;
import net.minestom.server.network.packet.server.play.DestroyEntitiesPacket;
import net.minestom.server.network.packet.server.play.EntityAnimationPacket;
import net.minestom.server.network.packet.server.play.EntityAttributesPacket;
import net.minestom.server.network.packet.server.play.EntityEquipmentPacket;
import net.minestom.server.network.packet.server.play.EntityHeadLookPacket;
import net.minestom.server.network.packet.server.play.EntityMetaDataPacket;
import net.minestom.server.network.packet.server.play.EntityPositionSyncPacket;
import net.minestom.server.network.packet.server.play.EntityRotationPacket;
import net.minestom.server.network.packet.server.play.EntityTeleportPacket;
import net.minestom.server.network.packet.server.play.EntityVelocityPacket;
import net.minestom.server.network.packet.server.play.JoinGamePacket;
import net.minestom.server.network.packet.server.play.SetPassengersPacket;
import net.minestom.server.network.packet.server.play.SpawnEntityPacket;
import net.minestom.server.network.packet.server.play.SystemChatPacket;
import net.minestom.server.network.packet.server.play.UnloadChunkPacket;
import proxy.ProxyNetworkContext;
import proxy.ProxyPackets;
import proxy.ProxySockets;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Seamless PLAY-state switch proxy template.
 * <p>
 * Assumes:
 * - all backends use the same native protocol version
 * - all backends share the same registries
 */
public final class PlaySwitchProxyTemplate {
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("0.0.0.0", 25565);
    private static final InetSocketAddress DEFAULT_BACKEND = new InetSocketAddress("127.0.0.1", 25566);
    private static final int BACKEND_CONNECT_TIMEOUT_MS = 3000;

    public static void main(String[] args) throws Exception {
        MinecraftServer.init();
        new PlaySwitchProxyTemplate().start();
    }

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final ServerSocketChannel server;

    public PlaySwitchProxyTemplate() throws IOException {
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
        System.out.println("Play switch proxy started on: " + ADDRESS + ", default backend: " + DEFAULT_BACKEND);

        Thread.startVirtualThread(this::listenCommands);
        Thread.startVirtualThread(this::listenConnections);

        stopLatch.await();
        ProxySockets.closeQuietly(server);
        System.out.println("Play switch proxy stopped");
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
        WAITING_PENDING_PLAY_READY
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
         * Tiny in-memory routing/switch state only.
         * No socket connect/read/write happens while holding it.
         */
        private final Object stateLock = new Object();

        private BackendConnection activeBackend;
        private BackendConnection pendingBackend;
        private SwitchState switchState = SwitchState.NONE;

        private final ArrayDeque<ServerPacket> pendingPlayPackets = new ArrayDeque<>();

        private int protocolVersion = MinecraftServer.PROTOCOL_VERSION;
        private String requestedHost = "localhost";
        private int requestedPort = 25565;

        private String username;
        private UUID profileId;

        private int clientPlayerEntityId = Integer.MIN_VALUE;

        private int nextClientEntityId = 1;

        private EntityIdMap activeEntityIds;
        private EntityIdMap pendingEntityIds;

        private final Set<Long> loadedChunks = new HashSet<>();
        private final Set<Integer> visibleEntityIds = new HashSet<>();

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
            sendToClientDirect(new SystemChatPacket(Component.text(message), false));
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
                pendingEntityIds = null;
                pendingPlayPackets.clear();
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
            final ClientPacket packet = PlaySwitchProxyTemplate.this.processClientPacket(this, rawPacket);
            if (packet == null) return;

            cacheClientMetadata(packet);

            final ClientPacket rewritten = rewriteClientPacketForBackend(packet);

            final BackendConnection current;
            synchronized (stateLock) {
                current = activeBackend;
            }
            if (current == null) {
                handleOfflineClientPacket(rewritten);
                return;
            }

            current.context.writeClient(rewritten);
            ProxyPackets.syncAfterClientForward(rewritten, clientContext, current.context);
        }

        private void handleBackendPacket(BackendConnection backend, ServerPacket rawPacket) {
            synchronized (stateLock) {
                if (backend == activeBackend) {
                    handleActiveBackendPacketLocked(rawPacket);
                    return;
                }

                if (backend != pendingBackend) {
                    return;
                }

                handlePendingBackendPacketLocked(backend, rawPacket);
            }
        }

        private void handleActiveBackendPacketLocked(ServerPacket rawPacket) {
            if (rawPacket instanceof JoinGamePacket joinGamePacket && activeEntityIds == null) {
                if (clientPlayerEntityId == Integer.MIN_VALUE) {
                    clientPlayerEntityId = joinGamePacket.entityId();
                    if (nextClientEntityId <= clientPlayerEntityId) {
                        nextClientEntityId = clientPlayerEntityId + 1;
                    }
                }
                activeEntityIds = new EntityIdMap(joinGamePacket.entityId(), clientPlayerEntityId);
            }

            final ServerPacket packet = PlaySwitchProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            sendToClient(packet);
        }

        private void handlePendingBackendPacketLocked(BackendConnection backend, ServerPacket rawPacket) {
            if (switchState == SwitchState.WAITING_PENDING_LOGIN_SUCCESS) {
                if (rawPacket instanceof LoginSuccessPacket) {
                    final ClientLoginAcknowledgedPacket loginAck = new ClientLoginAcknowledgedPacket();
                    backend.context.writeClient(loginAck);
                    ProxyPackets.syncAfterClientForward(loginAck, backend.context);

                    switchState = SwitchState.WAITING_PENDING_PLAY_READY;
                }
                return;
            }

            if (rawPacket instanceof SelectKnownPacksPacket selectKnownPacksPacket) {
                final ClientSelectKnownPacksPacket response = new ClientSelectKnownPacksPacket(selectKnownPacksPacket.entries());
                backend.context.writeClient(response);
                ProxyPackets.syncAfterClientForward(response, backend.context);
                return;
            }

            if (rawPacket instanceof FinishConfigurationPacket) {
                final ClientFinishConfigurationPacket finish = new ClientFinishConfigurationPacket();
                backend.context.writeClient(finish);
                ProxyPackets.syncAfterClientForward(finish, backend.context);
                return;
            }

            if (!(rawPacket instanceof ServerPacket.Play)) {
                return;
            }

            if (rawPacket instanceof JoinGamePacket joinGamePacket) {
                if (clientPlayerEntityId == Integer.MIN_VALUE) {
                    cancelSwitchLocked("Client player entity id is not initialized.");
                    return;
                }

                pendingEntityIds = new EntityIdMap(joinGamePacket.entityId(), clientPlayerEntityId);

                /*
                 * Do not send JoinGamePacket to the client during a seamless switch.
                 * We stay in PLAY and replay later play packets under the remapped id space.
                 */
                completePlaySwitchLocked();
                return;
            }

            final ServerPacket packet = PlaySwitchProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet != null) {
                pendingPlayPackets.addLast(packet);
            }
        }

        private void completePlaySwitchLocked() {
            final BackendConnection old = activeBackend;
            final BackendConnection next = pendingBackend;
            final EntityIdMap nextIds = pendingEntityIds;
            if (next == null || nextIds == null) return;

            sendCleanupPacketsLocked();

            activeBackend = next;
            pendingBackend = null;
            activeEntityIds = nextIds;
            pendingEntityIds = null;
            switchState = SwitchState.NONE;

            replayPendingPlayPacketsLocked();

            if (old != null && old != next) {
                silentCloseBackend(old);
            }

            sendSystemMessage("Connected to " + next.address + ".");
        }

        private void cancelSwitchLocked(String reason) {
            final BackendConnection next = pendingBackend;

            pendingBackend = null;
            pendingEntityIds = null;
            switchState = SwitchState.NONE;
            pendingPlayPackets.clear();

            if (next != null) {
                silentCloseBackend(next);
            }

            sendSystemMessage(reason);
        }

        private void sendCleanupPacketsLocked() {
            if (!visibleEntityIds.isEmpty()) {
                sendToClientDirect(new DestroyEntitiesPacket(List.copyOf(visibleEntityIds)));
            }

            if (!loadedChunks.isEmpty()) {
                for (long chunkIndex : loadedChunks) {
                    sendToClientDirect(new UnloadChunkPacket(
                            CoordConversion.chunkIndexGetX(chunkIndex),
                            CoordConversion.chunkIndexGetZ(chunkIndex)
                    ));
                }
            }

            visibleEntityIds.clear();
            loadedChunks.clear();
        }

        private void replayPendingPlayPacketsLocked() {
            while (!pendingPlayPackets.isEmpty()) {
                final ServerPacket packet = pendingPlayPackets.removeFirst();
                sendToClient(packet);
            }
        }

        private void sendToClient(ServerPacket packet) {
            if (packet == null || closed.get()) return;

            final ServerPacket rewritten = rewriteServerPacketForClient(packet);
            if (rewritten == null) return;

            observeServerPacket(rewritten);
            sendToClientDirect(rewritten);
        }

        private void sendToClientDirect(ServerPacket packet) {
            if (packet == null || closed.get()) return;

            clientContext.writeServer(packet);
            ProxyPackets.syncAfterServerForward(packet, clientContext);
        }

        private ServerPacket rewriteServerPacketForClient(ServerPacket packet) {
            final EntityIdMap map = activeEntityIds;
            if (map == null) {
                return packet;
            }

            if (!(packet instanceof ServerPacket.Play)) {
                return packet;
            }

            if (packet instanceof SpawnEntityPacket spawnEntityPacket) {
                return new SpawnEntityPacket(
                        map.toClient(spawnEntityPacket.entityId()),
                        spawnEntityPacket.uuid(),
                        spawnEntityPacket.type(),
                        spawnEntityPacket.position(),
                        spawnEntityPacket.headRot(),
                        spawnEntityPacket.data(),
                        spawnEntityPacket.velocity()
                );
            }

            if (packet instanceof DestroyEntitiesPacket destroyEntitiesPacket) {
                return new DestroyEntitiesPacket(map.toClientExistingOrSameList(destroyEntitiesPacket.entityIds()));
            }

            if (packet instanceof EntityTeleportPacket entityTeleportPacket) {
                return new EntityTeleportPacket(
                        map.toClient(entityTeleportPacket.entityId()),
                        entityTeleportPacket.position(),
                        entityTeleportPacket.delta(),
                        entityTeleportPacket.flags(),
                        entityTeleportPacket.onGround()
                );
            }

            if (packet instanceof EntityPositionSyncPacket entityPositionSyncPacket) {
                return new EntityPositionSyncPacket(
                        map.toClient(entityPositionSyncPacket.entityId()),
                        entityPositionSyncPacket.position(),
                        entityPositionSyncPacket.delta(),
                        entityPositionSyncPacket.yaw(),
                        entityPositionSyncPacket.pitch(),
                        entityPositionSyncPacket.onGround()
                );
            }

            if (packet instanceof EntityRotationPacket entityRotationPacket) {
                return new EntityRotationPacket(
                        map.toClient(entityRotationPacket.entityId()),
                        entityRotationPacket.yaw(),
                        entityRotationPacket.pitch(),
                        entityRotationPacket.onGround()
                );
            }

            if (packet instanceof EntityHeadLookPacket entityHeadLookPacket) {
                return new EntityHeadLookPacket(
                        map.toClient(entityHeadLookPacket.entityId()),
                        entityHeadLookPacket.yaw()
                );
            }

            if (packet instanceof EntityVelocityPacket entityVelocityPacket) {
                return new EntityVelocityPacket(
                        map.toClient(entityVelocityPacket.entityId()),
                        entityVelocityPacket.velocity()
                );
            }

            if (packet instanceof EntityEquipmentPacket entityEquipmentPacket) {
                return new EntityEquipmentPacket(
                        map.toClient(entityEquipmentPacket.entityId()),
                        entityEquipmentPacket.equipments()
                );
            }

            if (packet instanceof EntityMetaDataPacket entityMetaDataPacket) {
                return new EntityMetaDataPacket(
                        map.toClient(entityMetaDataPacket.entityId()),
                        entityMetaDataPacket.entries()
                );
            }

            if (packet instanceof EntityAttributesPacket entityAttributesPacket) {
                return new EntityAttributesPacket(
                        map.toClient(entityAttributesPacket.entityId()),
                        entityAttributesPacket.properties()
                );
            }

            if (packet instanceof EntityAnimationPacket entityAnimationPacket) {
                return new EntityAnimationPacket(
                        map.toClient(entityAnimationPacket.entityId()),
                        entityAnimationPacket.animation()
                );
            }

            if (packet instanceof SetPassengersPacket setPassengersPacket) {
                return new SetPassengersPacket(
                        map.toClient(setPassengersPacket.vehicleEntityId()),
                        map.toClientList(setPassengersPacket.passengersId())
                );
            }

            return packet;
        }

        private ClientPacket rewriteClientPacketForBackend(ClientPacket packet) {
            final EntityIdMap map = activeEntityIds;
            if (map == null) {
                return packet;
            }

            if (packet instanceof ClientInteractEntityPacket interactEntityPacket) {
                return new ClientInteractEntityPacket(
                        map.toBackend(interactEntityPacket.targetId()),
                        interactEntityPacket.type(),
                        interactEntityPacket.sneaking()
                );
            }

            return packet;
        }

        private void observeServerPacket(ServerPacket packet) {
            if (packet instanceof JoinGamePacket joinGamePacket) {
                clientPlayerEntityId = joinGamePacket.entityId();
                return;
            }

            if (packet instanceof ChunkDataPacket chunkDataPacket) {
                loadedChunks.add(CoordConversion.chunkIndex(chunkDataPacket.chunkX(), chunkDataPacket.chunkZ()));
                return;
            }

            if (packet instanceof UnloadChunkPacket unloadChunkPacket) {
                loadedChunks.remove(CoordConversion.chunkIndex(unloadChunkPacket.chunkX(), unloadChunkPacket.chunkZ()));
                return;
            }

            if (packet instanceof DestroyEntitiesPacket destroyEntitiesPacket) {
                for (Integer entityId : destroyEntitiesPacket.entityIds()) {
                    visibleEntityIds.remove(entityId);
                }
                return;
            }

            if (packet instanceof SpawnEntityPacket spawnEntityPacket) {
                if (spawnEntityPacket.entityId() != clientPlayerEntityId) {
                    visibleEntityIds.add(spawnEntityPacket.entityId());
                }
            }
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
                    pendingEntityIds = null;
                    pendingPlayPackets.clear();
                    switchState = SwitchState.NONE;
                    systemMessage = "Backend " + backend.address + " is offline.";
                } else if (backend == activeBackend) {
                    activeBackend = null;
                    disconnectMessage = "Backend " + backend.address + " closed.";
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

                pendingEntityIds = null;
                pendingPlayPackets.clear();
            }

            silentCloseBackend(active);
            if (pending != active) {
                silentCloseBackend(pending);
            }

            ProxySockets.closeQuietly(client);
        }

        /**
         * Backend<->client entity id remap for one backend.
         * <p>
         * The local player gets a stable client-facing id for the whole session.
         * Other backend entity ids are lazily assigned client-facing ids.
         */
        final class EntityIdMap {
            private final int backendPlayerEntityId;
            private final int clientPlayerEntityId;

            private final Int2IntOpenHashMap backendToClient = new Int2IntOpenHashMap();
            private final Int2IntOpenHashMap clientToBackend = new Int2IntOpenHashMap();

            EntityIdMap(int backendPlayerEntityId, int clientPlayerEntityId) {
                this.backendPlayerEntityId = backendPlayerEntityId;
                this.clientPlayerEntityId = clientPlayerEntityId;

                backendToClient.defaultReturnValue(Integer.MIN_VALUE);
                clientToBackend.defaultReturnValue(Integer.MIN_VALUE);

                backendToClient.put(backendPlayerEntityId, clientPlayerEntityId);
                clientToBackend.put(clientPlayerEntityId, backendPlayerEntityId);
            }

            int toClient(int backendEntityId) {
                final int mapped = backendToClient.get(backendEntityId);
                if (mapped != Integer.MIN_VALUE) {
                    return mapped;
                }

                int clientEntityId;
                do {
                    clientEntityId = nextClientEntityId++;
                } while (clientEntityId == clientPlayerEntityId);

                backendToClient.put(backendEntityId, clientEntityId);
                clientToBackend.put(clientEntityId, backendEntityId);
                return clientEntityId;
            }

            int toClientExistingOrSame(int backendEntityId) {
                final int mapped = backendToClient.get(backendEntityId);
                return mapped != Integer.MIN_VALUE ? mapped : backendEntityId;
            }

            List<Integer> toClientList(List<Integer> backendEntityIds) {
                List<Integer> rewritten = null;

                for (int i = 0; i < backendEntityIds.size(); i++) {
                    final int original = backendEntityIds.get(i);
                    final int mapped = toClient(original);

                    if (mapped != original) {
                        if (rewritten == null) {
                            rewritten = new ArrayList<>(backendEntityIds);
                        }
                        rewritten.set(i, mapped);
                    }
                }

                return rewritten != null ? List.copyOf(rewritten) : backendEntityIds;
            }

            List<Integer> toClientExistingOrSameList(List<Integer> backendEntityIds) {
                List<Integer> rewritten = null;

                for (int i = 0; i < backendEntityIds.size(); i++) {
                    final int original = backendEntityIds.get(i);
                    final int mapped = toClientExistingOrSame(original);

                    if (mapped != original) {
                        if (rewritten == null) {
                            rewritten = new ArrayList<>(backendEntityIds);
                        }
                        rewritten.set(i, mapped);
                    }
                }

                return rewritten != null ? List.copyOf(rewritten) : backendEntityIds;
            }

            int toBackend(int clientEntityId) {
                final int mapped = clientToBackend.get(clientEntityId);
                return mapped != Integer.MIN_VALUE ? mapped : clientEntityId;
            }
        }
    }
}