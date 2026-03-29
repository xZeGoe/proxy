package proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;

public final class ProxySockets {
    private ProxySockets() {
    }

    public static SocketChannel connect(InetSocketAddress address, int timeoutMs) throws IOException {
        SocketChannel channel = SocketChannel.open(StandardProtocolFamily.INET);
        boolean success = false;
        try {
            configure(channel);
            channel.socket().connect(address, timeoutMs);
            success = true;
            return channel;
        } finally {
            if (!success) {
                closeQuietly(channel);
            }
        }
    }

    public static void configure(SocketChannel channel) throws IOException {
        channel.configureBlocking(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setKeepAlive(true);
    }

    public static void closeQuietly(Channel channel) {
        if (channel == null) return;
        try {
            channel.close();
        } catch (IOException ignored) {
        }
    }
}