package template;

import net.kyori.adventure.text.Component;
import proxy.*;
import net.minestom.server.MinecraftServer;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bare proxy example.
 * <p>
 * Single backend, no switching.
 */
public final class ProxyTemplate {
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("0.0.0.0", 25565);
    private static final InetSocketAddress BACKEND_ADDRESS = new InetSocketAddress("127.0.0.1", 25566);
    private static final int BACKEND_CONNECT_TIMEOUT_MS = 3000;

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final ServerSocketChannel server;

    public static void main(String[] args) throws Exception {
        MinecraftServer.init();
        new ProxyTemplate().start();
    }

    public ProxyTemplate() throws IOException {
        this.server = ServerSocketChannel.open(StandardProtocolFamily.INET);
    }


    ClientPacket processClientPacket(Connection connection, ClientPacket packet) {
        // System.out.println("C -> S : " + packet);
        return packet;
    }

    ServerPacket processServerPacket(Connection connection, ServerPacket packet) {
        // System.out.println("S -> C : " + packet);
        return packet;
    }

    public void start() throws Exception {
        server.bind(ADDRESS);
        System.out.println("Proxy started on: " + ADDRESS + ", backend: " + BACKEND_ADDRESS);

        Thread.startVirtualThread(this::listenCommands);
        Thread.startVirtualThread(this::listenConnections);

        stopLatch.await();
        ProxySockets.closeQuietly(server);
        System.out.println("Proxy stopped");
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

    final class Connection {
        private final String remoteAddress;
        private final SocketChannel client;
        private final ProxyNetworkContext clientContext = new ProxyNetworkContext();

        private final SocketChannel backendChannel;
        private final ProxyNetworkContext backendContext;

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicBoolean backendClosed = new AtomicBoolean(false);

        Connection(SocketChannel client) throws IOException {
            this.client = client;
            ProxySockets.configure(client);
            this.remoteAddress = String.valueOf(client.getRemoteAddress());

            SocketChannel backendChannel = null;
            ProxyNetworkContext backendContext = null;
            try {
                backendChannel = ProxySockets.connect(BACKEND_ADDRESS, BACKEND_CONNECT_TIMEOUT_MS);
                backendContext = new ProxyNetworkContext();
                System.out.println("Accepted " + remoteAddress + ", connected backend " + BACKEND_ADDRESS);
            } catch (IOException e) {
                ProxySockets.closeQuietly(backendChannel);
                System.out.println("Accepted " + remoteAddress + ", backend unavailable " + BACKEND_ADDRESS + " (" + e.getMessage() + ")");
            }

            this.backendChannel = backendChannel;
            this.backendContext = backendContext;
        }

        void start() {
            Thread.startVirtualThread(this::clientReadLoop);
            Thread.startVirtualThread(this::clientWriteLoop);

            if (backendChannel != null && backendContext != null) {
                Thread.startVirtualThread(this::backendReadLoop);
                Thread.startVirtualThread(this::backendWriteLoop);
            }
        }

        void sendToClient(ServerPacket packet) {
            if (closed.get() || packet == null) return;
            clientContext.writeServer(packet);
            ProxyPackets.syncAfterServerForward(packet, clientContext);
        }

        void disconnect(String message) {
            if (closed.get()) return;

            final ConnectionState state = clientContext.writeState();
            if (state == ConnectionState.LOGIN || state == ConnectionState.CONFIGURATION || state == ConnectionState.PLAY) {
                clientContext.writeServer(ProxyPackets.disconnectPacket(state, Component.text(message)));
                closeSoon();
            } else {
                close();
            }
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

        private void backendReadLoop() {
            while (!closed.get()) {
                boolean ok = backendContext.readServer(buffer -> {
                    try {
                        buffer.readChannel(backendChannel);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleServerPacket);

                if (!ok) {
                    handleBackendClosed();
                    return;
                }
            }
        }

        private void backendWriteLoop() {
            while (!closed.get()) {
                boolean ok = backendContext.flush(buffer -> {
                    try {
                        buffer.writeChannel(backendChannel);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (!ok) {
                    handleBackendClosed();
                    return;
                }
            }
        }

        private void handleClientPacket(ClientPacket rawPacket) {
            final ClientPacket packet = ProxyTemplate.this.processClientPacket(this, rawPacket);
            if (packet == null) return;

            if (backendContext == null || backendChannel == null) {
                ProxyPackets.syncAfterClientForward(packet, clientContext);

                if (packet instanceof StatusRequestPacket) {
                    sendToClient(ProxyPackets.offlineResponse("Backend is offline"));
                    return;
                }
                if (packet instanceof ClientPingRequestPacket(long number)) {
                    sendToClient(new PingResponsePacket(number));
                    closeSoon();
                    return;
                }
                if (packet instanceof ClientLoginStartPacket) {
                    disconnect("Backend is offline.");
                }
                return;
            }

            backendContext.writeClient(packet);
            ProxyPackets.syncAfterClientForward(packet, clientContext, backendContext);
        }

        private void handleServerPacket(ServerPacket rawPacket) {
            final ServerPacket packet = ProxyTemplate.this.processServerPacket(this, rawPacket);
            if (packet == null) return;

            sendToClient(packet);
        }

        private void handleBackendClosed() {
            if (closed.get()) return;
            if (!backendClosed.compareAndSet(false, true)) return;
            System.out.println("Backend closed for " + remoteAddress);
            disconnect("Backend connection closed.");
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
            if (backendContext != null) backendContext.close();

            ProxySockets.closeQuietly(client);
            ProxySockets.closeQuietly(backendChannel);
        }
    }
}