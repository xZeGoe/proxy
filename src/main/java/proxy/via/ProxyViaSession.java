package proxy.via;

import com.viaversion.viaversion.api.Via;
import com.viaversion.viaversion.api.connection.ProtocolInfo;
import com.viaversion.viaversion.api.protocol.ProtocolPathEntry;
import com.viaversion.viaversion.api.protocol.packet.PacketWrapper;
import com.viaversion.viaversion.api.protocol.packet.State;
import com.viaversion.viaversion.api.protocol.version.ProtocolVersion;
import com.viaversion.viaversion.api.type.Types;
import com.viaversion.viaversion.connection.UserConnectionImpl;
import com.viaversion.viaversion.protocol.ProtocolPipelineImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.util.ReferenceCountUtil;
import net.minestom.server.network.ConnectionState;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public final class ProxyViaSession {
    public record TranslationResult(byte[] body, List<byte[]> injectedBodies) {
    }

    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final Object lock = new Object();

    private final UserConnectionImpl userConnection;

    private final ProtocolVersion clientVersion;
    private final ProtocolVersion serverVersion;
    private final boolean translationRequired;
    private final boolean supported;

    private final ArrayDeque<byte[]> injectedClientboundPackets = new ArrayDeque<>();
    private final ArrayDeque<byte[]> injectedServerboundPackets = new ArrayDeque<>();

    private boolean loggedIn;

    public ProxyViaSession(int clientProtocolVersion, int nativeProtocolVersion, ConnectionState initialState) {
        installCapturePipeline(channel);

        this.userConnection = new UserConnectionImpl(channel, false);
        this.clientVersion = ProtocolVersion.getProtocol(clientProtocolVersion);
        this.serverVersion = ProtocolVersion.getProtocol(nativeProtocolVersion);
        this.translationRequired = !clientVersion.equalTo(serverVersion);

        if (!translationRequired) {
            this.supported = true;
            return;
        }

        new ProtocolPipelineImpl(userConnection);

        final ProtocolInfo info = userConnection.getProtocolInfo();
        info.setProtocolVersion(clientVersion);
        info.setServerProtocolVersion(serverVersion);
        info.setState(mapState(initialState));

        final List<ProtocolPathEntry> path =
                Via.getManager().getProtocolManager().getProtocolPath(clientVersion, serverVersion);

        if (path == null || path.isEmpty()) {
            this.supported = false;
            userConnection.setActive(false);
            return;
        }

        for (ProtocolPathEntry entry : path) {
            info.getPipeline().add(entry.protocol());
            entry.protocol().init(userConnection);
        }

        userConnection.setActive(true);
        this.supported = true;
    }

    public boolean supported() {
        return supported;
    }

    public boolean translationRequired() {
        return translationRequired;
    }

    public void setState(ConnectionState state) {
        synchronized (lock) {
            if (!translationRequired) return;
            userConnection.getProtocolInfo().setState(mapState(state));
        }
    }

    public TranslationResult translateServerbound(byte[] body) {
        synchronized (lock) {
            if (!translationRequired) {
                return new TranslationResult(body, List.of());
            }
            if (!supported) {
                return new TranslationResult(null, List.of());
            }

            final ByteBuf buf = Unpooled.buffer(Math.max(256, body.length + 32));
            buf.writeBytes(body);

            try {
                if (!userConnection.checkServerboundPacket(buf.readableBytes())) {
                    return new TranslationResult(null, drainInjectedServerbound());
                }

                userConnection.transformServerbound(buf, DecoderException::new);

                byte[] out = new byte[buf.readableBytes()];
                buf.readBytes(out);
                return new TranslationResult(out, drainInjectedServerbound());
            } catch (DecoderException | EncoderException e) {
                if (isViaCancel(e)) {
                    return new TranslationResult(null, drainInjectedServerbound());
                }
                throw e;
            } finally {
                buf.release();
            }
        }
    }

    public TranslationResult translateClientbound(byte[] body) {
        synchronized (lock) {
            if (!translationRequired) {
                return new TranslationResult(body, List.of());
            }
            if (!supported) {
                return new TranslationResult(null, List.of());
            }

            final ByteBuf buf = Unpooled.buffer(Math.max(256, body.length + 32));
            buf.writeBytes(body);

            try {
                if (!userConnection.checkClientboundPacket()) {
                    return new TranslationResult(null, drainInjectedClientbound());
                }

                userConnection.transformClientbound(buf, EncoderException::new);

                byte[] out = new byte[buf.readableBytes()];
                buf.readBytes(out);
                return new TranslationResult(out, drainInjectedClientbound());
            } catch (DecoderException | EncoderException e) {
                if (isViaCancel(e)) {
                    return new TranslationResult(null, drainInjectedClientbound());
                }
                throw e;
            } finally {
                buf.release();
            }
        }
    }

    public void onLoginSuccess(String username, UUID uuid) {
        synchronized (lock) {
            if (!translationRequired || loggedIn) return;

            final ProtocolInfo info = userConnection.getProtocolInfo();
            info.setUsername(username);
            info.setUuid(uuid);

            Via.getManager().getConnectionManager().onLoginSuccess(userConnection);
            loggedIn = true;
        }
    }

    public void close() {
        synchronized (lock) {
            try {
                if (loggedIn) {
                    Via.getManager().getConnectionManager().onDisconnect(userConnection);
                }
            } catch (Throwable ignored) {
            }
            channel.close();
        }
    }

    private void installCapturePipeline(EmbeddedChannel channel) {
        /*
         * Server-side/front-end UserConnectionImpl.sendRawPacketNow(...)
         * writes through pipeline.context(encoderName).writeAndFlush(...).
         *
         * So the capture handler must be BEFORE the named encoder context.
         */
        channel.pipeline().addLast("proxy-via-out-capture", new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                try {
                    if (msg instanceof ByteBuf buf) {
                        byte[] bytes = new byte[buf.readableBytes()];
                        buf.getBytes(buf.readerIndex(), bytes);
                        injectedClientboundPackets.add(bytes);
                    }
                    promise.setSuccess();
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
        });

        channel.pipeline().addLast(ProxyViaInjector.ENCODER_NAME, new ChannelOutboundHandlerAdapter());

        /*
         * For injected serverbound packets, Via fires channelRead from decoderName,
         * so the capture handler must be AFTER the named decoder context.
         */
        channel.pipeline().addLast(ProxyViaInjector.DECODER_NAME, new ChannelInboundHandlerAdapter());

        channel.pipeline().addLast("proxy-via-in-capture", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                try {
                    if (msg instanceof ByteBuf buf) {
                        byte[] bytes = unwrapInjectedServerbound(buf);
                        if (bytes.length > 0) {
                            injectedServerboundPackets.add(bytes);
                        }
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
        });
    }

    private byte[] unwrapInjectedServerbound(ByteBuf buf) {
        /*
         * Via wraps injected serverbound packets as:
         *   PASSTHROUGH_ID + UUID token + real packet body
         */
        final ByteBuf copy = buf.retainedDuplicate();
        try {
            final int packetId = Types.VAR_INT.readPrimitive(copy);
            if (packetId == PacketWrapper.PASSTHROUGH_ID) {
                Types.UUID.read(copy);
            } else {
                copy.readerIndex(buf.readerIndex());
            }

            final byte[] bytes = new byte[copy.readableBytes()];
            copy.readBytes(bytes);
            return bytes;
        } finally {
            copy.release();
        }
    }

    private List<byte[]> drainInjectedClientbound() {
        return drainQueue(injectedClientboundPackets);
    }

    private List<byte[]> drainInjectedServerbound() {
        return drainQueue(injectedServerboundPackets);
    }

    private static List<byte[]> drainQueue(ArrayDeque<byte[]> queue) {
        if (queue.isEmpty()) return List.of();

        List<byte[]> out = new ArrayList<>(queue.size());
        while (!queue.isEmpty()) {
            out.add(queue.removeFirst());
        }
        return out;
    }

    private static boolean isViaCancel(Throwable throwable) {
        while (throwable != null) {
            final String name = throwable.getClass().getName();
            if (name.contains("CancelException")
                    || name.contains("CancelEncoderException")
                    || name.contains("CancelDecoderException")) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private static State mapState(ConnectionState state) {
        return switch (state) {
            case HANDSHAKE -> State.HANDSHAKE;
            case STATUS -> State.STATUS;
            case LOGIN -> State.LOGIN;
            case CONFIGURATION -> State.CONFIGURATION;
            case PLAY -> State.PLAY;
        };
    }
}