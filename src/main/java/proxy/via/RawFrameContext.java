package proxy.via;

import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketVanilla;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static net.minestom.server.network.NetworkBuffer.RAW_BYTES;
import static net.minestom.server.network.NetworkBuffer.VAR_INT;

/**
 * Raw framed packet context for the client-facing Via path.
 * <p>
 * Frame format:
 * - outer packet length varint
 * - packet body (packet id varint + payload)
 */
public final class RawFrameContext {
    private static final int MAX_VAR_INT_SIZE = 5;

    private final MpmcUnboundedXaddArrayQueue<NetworkBuffer> frameWriteQueue = new MpmcUnboundedXaddArrayQueue<>(256);

    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition writeCondition = writeLock.newCondition();

    private final NetworkBuffer readBuffer =
            NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.process());

    private NetworkBuffer writeLeftover;
    private volatile boolean closing;

    public void close() {
        this.closing = true;
        signalWrite();
    }

    public void feedRawBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return;
        readBuffer.ensureWritable(bytes.length);
        readBuffer.write(RAW_BYTES, bytes);
    }

    public boolean readFrames(Consumer<NetworkBuffer> reader, Consumer<byte[]> bodyConsumer) {
        try {
            reader.accept(readBuffer);
        } catch (Exception e) {
            return false;
        }

        consumeAvailableFrames(bodyConsumer);
        readBuffer.compact();
        return true;
    }

    public void consumeBufferedFrames(Consumer<byte[]> bodyConsumer) {
        consumeAvailableFrames(bodyConsumer);
        readBuffer.compact();
    }

    private void consumeAvailableFrames(Consumer<byte[]> bodyConsumer) {
        while (true) {
            final long beginMark = readBuffer.readIndex();

            final int frameLength;
            try {
                frameLength = readBuffer.read(VAR_INT);
            } catch (IndexOutOfBoundsException e) {
                readBuffer.readIndex(beginMark);
                return;
            }

            final long bodyStart = readBuffer.readIndex();
            if (bodyStart > readBuffer.writeIndex()) {
                readBuffer.readIndex(beginMark);
                return;
            }

            if (frameLength < 0 || frameLength > ServerFlag.MAX_PACKET_SIZE) {
                throw new RuntimeException("Invalid frame length: " + frameLength
                        + ", unreadPrefix=[" + debugPrefix(beginMark, 16) + "]");
            }

            if (readBuffer.readableBytes() < frameLength) {
                readBuffer.readIndex(beginMark);

                final long frameHeaderLength = bodyStart - beginMark;
                final long requiredCapacity = frameHeaderLength + frameLength;
                if (requiredCapacity > readBuffer.capacity()) {
                    readBuffer.resize(requiredCapacity);
                }
                return;
            }

            byte[] body = new byte[frameLength];
            readBuffer.copyTo(bodyStart, body, 0, frameLength);
            readBuffer.readIndex(bodyStart + frameLength);

            bodyConsumer.accept(body);
        }
    }

    public void writeBody(byte[] body) {
        if (body == null || closing) return;

        final NetworkBuffer frame = PacketVanilla.PACKET_POOL.get();
        boolean success = false;
        try {
            frame.clear();

            while (true) {
                try {
                    frame.write(VAR_INT, body.length);
                    frame.write(RAW_BYTES, body);
                    success = true;
                    break;
                } catch (IndexOutOfBoundsException e) {
                    frame.clear();
                    final long required = body.length + MAX_VAR_INT_SIZE;
                    frame.resize(Math.max(frame.capacity() * 2, required));
                }
            }

            frameWriteQueue.add(frame);
            signalWrite();
        } finally {
            if (!success) {
                PacketVanilla.PACKET_POOL.add(frame);
            }
        }
    }

    public boolean flush(Consumer<NetworkBuffer> writer) {
        NetworkBuffer frame = this.writeLeftover;

        if (frame == null) {
            if (frameWriteQueue.isEmpty()) {
                writeLock.lock();
                try {
                    while (frameWriteQueue.isEmpty() && !closing) {
                        writeCondition.await();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } finally {
                    writeLock.unlock();
                }
                if (closing && frameWriteQueue.isEmpty()) {
                    return false;
                }
            }

            frame = frameWriteQueue.poll();
            if (frame == null) {
                return !closing;
            }
        }

        try {
            writer.accept(frame);
        } catch (Exception e) {
            if (frame == this.writeLeftover) {
                this.writeLeftover = null;
            }
            PacketVanilla.PACKET_POOL.add(frame);
            return false;
        }

        if (frame.readableBytes() == 0) {
            this.writeLeftover = null;
            PacketVanilla.PACKET_POOL.add(frame);
        } else {
            this.writeLeftover = frame;
        }
        return true;
    }

    private void signalWrite() {
        writeLock.lock();
        try {
            writeCondition.signal();
        } finally {
            writeLock.unlock();
        }
    }

    private String debugPrefix(long startIndex, int maxBytes) {
        final long available = Math.max(0, readBuffer.writeIndex() - startIndex);
        final int length = (int) Math.min(maxBytes, available);

        byte[] bytes = new byte[length];
        if (length > 0) {
            readBuffer.copyTo(startIndex, bytes, 0, length);
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) builder.append(' ');
            builder.append(String.format("%02x", bytes[i] & 0xFF));
        }
        return builder.toString();
    }
}