package proxy.via;

import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.NetworkBuffer;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static net.minestom.server.network.NetworkBuffer.VAR_INT;

/**
 * Reads exactly the first full client frame and keeps any already-received trailing raw bytes.
 * <p>
 * Used only to choose between native of via frontend paths
 */
public final class InitialClientFrameReader {
    private final NetworkBuffer buffer = NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.process());

    public record Result(byte[] firstBody, byte[] remainingRaw) {
    }

    public Result readFirstFrame(SocketChannel channel) throws IOException {
        while (true) {
            final Result extracted = tryExtract();
            if (extracted != null) {
                return extracted;
            }

            buffer.compact();
            buffer.readChannel(channel);
        }
    }

    private Result tryExtract() {
        final long beginMark = buffer.readIndex();

        final int frameLength;
        try {
            frameLength = buffer.read(VAR_INT);
        } catch (IndexOutOfBoundsException e) {
            buffer.readIndex(beginMark);
            return null;
        }

        final long bodyStart = buffer.readIndex();
        if (bodyStart > buffer.writeIndex()) {
            buffer.readIndex(beginMark);
            return null;
        }

        if (frameLength < 0 || frameLength > ServerFlag.MAX_PACKET_SIZE) {
            throw new RuntimeException("Invalid first frame length: " + frameLength);
        }

        if (buffer.readableBytes() < frameLength) {
            buffer.readIndex(beginMark);

            final long frameHeaderLength = bodyStart - beginMark;
            final long requiredCapacity = frameHeaderLength + frameLength;
            if (requiredCapacity > buffer.capacity()) {
                buffer.resize(requiredCapacity);
            }
            return null;
        }

        byte[] firstBody = new byte[frameLength];
        buffer.copyTo(bodyStart, firstBody, 0, frameLength);
        buffer.readIndex(bodyStart + frameLength);

        final int remainingLength = (int) buffer.readableBytes();
        byte[] remainingRaw = new byte[remainingLength];
        if (remainingLength > 0) {
            buffer.copyTo(buffer.readIndex(), remainingRaw, 0, remainingLength);
        }

        return new Result(firstBody, remainingRaw);
    }
}