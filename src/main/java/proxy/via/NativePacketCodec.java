package proxy.via;

import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.packet.PacketRegistry;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.server.ServerPacket;

import static net.minestom.server.network.NetworkBuffer.VAR_INT;

public final class NativePacketCodec {
    private NativePacketCodec() {
    }

    public static ClientPacket readClient(ConnectionState state, byte[] bodyBytes) {
        return read(PacketVanilla.CLIENT_PACKET_PARSER, state, bodyBytes);
    }

    public static ServerPacket readServer(ConnectionState state, byte[] bodyBytes) {
        return read(PacketVanilla.SERVER_PACKET_PARSER, state, bodyBytes);
    }

    public static byte[] writeClientBody(ConnectionState state, ClientPacket packet) {
        return writeBody(PacketVanilla.CLIENT_PACKET_PARSER, state, packet);
    }

    public static byte[] writeServerBody(ConnectionState state, ServerPacket packet) {
        return writeBody(PacketVanilla.SERVER_PACKET_PARSER, state, packet);
    }

    private static <T> T read(PacketParser<T> parser, ConnectionState state, byte[] bodyBytes) {
        final NetworkBuffer body = NetworkBuffer.wrap(bodyBytes, 0, bodyBytes.length, MinecraftServer.process());
        final PacketRegistry<T> registry = parser.stateRegistry(state);
        final int packetId = body.read(VAR_INT);
        final PacketRegistry.PacketInfo<T> packetInfo = registry.packetInfo(packetId);

        try {
            final T packet = packetInfo.serializer().read(body);
            if (body.readableBytes() != 0) {
                throw new IllegalStateException("Packet " + packetInfo.packetClass().getSimpleName() + " not fully read");
            }
            return packet;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to read packet class " + packetInfo.packetClass().getName()
                            + " (id " + packetId + ", state " + state + ")",
                    e
            );
        }
    }

    private static <T> byte[] writeBody(PacketParser<T> parser, ConnectionState state, T packet) {
        final PacketRegistry<T> registry = parser.stateRegistry(state);
        final PacketRegistry.PacketInfo<T> packetInfo = registry.packetInfo(packet);

        final NetworkBuffer body = NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.process());
        try {
            body.write(VAR_INT, packetInfo.id());
            body.write(packetInfo.serializer(), packet);

            final byte[] bytes = new byte[(int) body.writeIndex()];
            body.copyTo(0, bytes, 0, bytes.length);
            return bytes;
        } catch (Exception e) {
            throw new RuntimeException("Failed to write packet class " + packet.getClass().getName() + " in state " + state, e);
        }
    }
}