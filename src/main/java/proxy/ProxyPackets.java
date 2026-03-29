package proxy;

import net.kyori.adventure.text.Component;
import net.minestom.server.MinecraftServer;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.play.ClientConfigurationAckPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.DisconnectPacket;
import net.minestom.server.network.packet.server.configuration.FinishConfigurationPacket;
import net.minestom.server.network.packet.server.login.LoginDisconnectPacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.play.StartConfigurationPacket;
import net.minestom.server.network.packet.server.status.ResponsePacket;

public final class ProxyPackets {
    private ProxyPackets() {
    }

    public static void syncAfterClientForward(ClientPacket packet, ProxyNetworkContext... contexts) {
        final ConnectionState nextState = nextStateAfterClientForward(packet);
        if (nextState == null) return;

        for (ProxyNetworkContext context : contexts) {
            if (context != null) {
                context.states(nextState);
            }
        }
    }

    public static void syncAfterServerForward(ServerPacket packet, ProxyNetworkContext... contexts) {
        final ConnectionState nextState = nextStateAfterServerForward(packet);
        if (nextState == null) return;

        for (ProxyNetworkContext context : contexts) {
            if (context != null) {
                context.writeState(nextState);
            }
        }
    }

    public static ServerPacket disconnectPacket(ConnectionState state, Component reason) {
        if (state == ConnectionState.LOGIN) {
            return new LoginDisconnectPacket(reason);
        }
        if (state == ConnectionState.CONFIGURATION || state == ConnectionState.PLAY) {
            return new DisconnectPacket(reason);
        }
        throw new IllegalStateException("Cannot send a disconnect packet in state " + state);
    }

    public static ResponsePacket offlineResponse(String description) {
        return statusResponse(description, 0, 0);
    }

    public static ResponsePacket statusResponse(String description, int onlinePlayers, int maxPlayers) {
        return new ResponsePacket(statusJson(description, onlinePlayers, maxPlayers));
    }

    public static String statusJson(String description, int onlinePlayers, int maxPlayers) {
        return """
                {
                  "version": {
                    "name": "%s",
                    "protocol": %s
                  },
                  "players": {
                    "max": %s,
                    "online": %s
                  },
                  "description": {
                    "text": "%s"
                  },
                  "enforcesSecureChat": false,
                  "previewsChat": false
                }
                """.formatted(
                MinecraftServer.VERSION_NAME,
                MinecraftServer.PROTOCOL_VERSION,
                maxPlayers,
                onlinePlayers,
                jsonEscape(description)
        );
    }

    public static String jsonEscape(String input) {
        StringBuilder builder = new StringBuilder(input.length() + 16);
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '\\' -> builder.append("\\\\");
                case '"' -> builder.append("\\\"");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (c <= 0x1F) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
                }
            }
        }
        return builder.toString();
    }

    private static ConnectionState nextStateAfterClientForward(ClientPacket packet) {
        if (packet instanceof ClientHandshakePacket handshakePacket) {
            return switch (handshakePacket.intent()) {
                case STATUS -> ConnectionState.STATUS;
                case LOGIN, TRANSFER -> ConnectionState.LOGIN;
            };
        }
        if (packet instanceof ClientLoginAcknowledgedPacket) {
            return ConnectionState.CONFIGURATION;
        }
        if (packet instanceof ClientConfigurationAckPacket) {
            return ConnectionState.CONFIGURATION;
        }
        if (packet instanceof ClientFinishConfigurationPacket) {
            return ConnectionState.PLAY;
        }
        return null;
    }

    private static ConnectionState nextStateAfterServerForward(ServerPacket packet) {
        return switch (packet) {
            case LoginSuccessPacket ignored -> ConnectionState.CONFIGURATION;
            case StartConfigurationPacket ignored -> ConnectionState.CONFIGURATION;
            case FinishConfigurationPacket ignored -> ConnectionState.PLAY;
            default -> null;
        };
    }
}