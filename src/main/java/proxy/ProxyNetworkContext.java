package proxy;

import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketReading;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;

public final class ProxyNetworkContext {
    private final AtomicReference<ConnectionState> readStateRef = new AtomicReference<>(ConnectionState.HANDSHAKE);
    private final AtomicReference<ConnectionState> writeStateRef = new AtomicReference<>(ConnectionState.HANDSHAKE);

    private final MpmcUnboundedXaddArrayQueue<QueuedPacket> writeQueue = new MpmcUnboundedXaddArrayQueue<>(256);

    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition writeCondition = writeLock.newCondition();

    private final NetworkBuffer readBuffer =
            NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.process());

    private NetworkBuffer writeLeftover;
    private volatile boolean closing;

    public ConnectionState readState() {
        return readStateRef.get();
    }

    public void readState(ConnectionState state) {
        readStateRef.set(state);
    }

    public ConnectionState writeState() {
        return writeStateRef.get();
    }

    public void writeState(ConnectionState state) {
        writeStateRef.set(state);
    }

    public void states(ConnectionState state) {
        readStateRef.set(state);
        writeStateRef.set(state);
    }

    public void close() {
        this.closing = true;
        signalWrite();
    }

    public void feedRawBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return;
        readBuffer.ensureWritable(bytes.length);
        readBuffer.write(NetworkBuffer.RAW_BYTES, bytes);
    }

    public void consumeBufferedClients(Consumer<ClientPacket> consumer) {
        while (readBuffer.readableBytes() > 0) {
            final PacketReading.Result<ClientPacket> result;
            try {
                result = PacketReading.readClients(readBuffer, readStateRef.get(), false);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }

            switch (result) {
                case PacketReading.Result.Empty<ClientPacket> ignored -> {
                    return;
                }
                case PacketReading.Result.Failure<ClientPacket> failure -> {
                    readBuffer.resize(failure.requiredCapacity());
                    return;
                }
                case PacketReading.Result.Success<ClientPacket> success -> {
                    for (PacketReading.ParsedPacket<ClientPacket> parsed : success.packets()) {
                        readStateRef.set(parsed.nextState());
                        consumer.accept(parsed.packet());
                    }
                    readBuffer.compact();
                }
            }
        }
    }

    public boolean readClient(Consumer<NetworkBuffer> reader, Consumer<ClientPacket> consumer) {
        try {
            reader.accept(readBuffer);
        } catch (Exception e) {
            return false;
        }

        final PacketReading.Result<ClientPacket> result;
        try {
            result = PacketReading.readClients(readBuffer, readStateRef.get(), false);
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        }

        switch (result) {
            case PacketReading.Result.Empty<ClientPacket> ignored -> {
            }
            case PacketReading.Result.Failure<ClientPacket> failure -> readBuffer.resize(failure.requiredCapacity());
            case PacketReading.Result.Success<ClientPacket> success -> {
                for (PacketReading.ParsedPacket<ClientPacket> parsed : success.packets()) {
                    readStateRef.set(parsed.nextState());
                    consumer.accept(parsed.packet());
                }
                readBuffer.compact();
            }
        }
        return true;
    }

    public boolean readServer(Consumer<NetworkBuffer> reader, Consumer<ServerPacket> consumer) {
        try {
            reader.accept(readBuffer);
        } catch (Exception e) {
            return false;
        }

        final PacketReading.Result<ServerPacket> result;
        try {
            result = PacketReading.readServers(readBuffer, readStateRef.get(), false);
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        }

        switch (result) {
            case PacketReading.Result.Empty<ServerPacket> ignored -> {
            }
            case PacketReading.Result.Failure<ServerPacket> failure -> readBuffer.resize(failure.requiredCapacity());
            case PacketReading.Result.Success<ServerPacket> success -> {
                for (PacketReading.ParsedPacket<ServerPacket> parsed : success.packets()) {
                    readStateRef.set(parsed.nextState());
                    consumer.accept(parsed.packet());
                }
                readBuffer.compact();
            }
        }
        return true;
    }

    public void writeClient(ClientPacket packet) {
        if (packet == null || closing) return;
        writeQueue.add(new ClientQueuedPacket(writeStateRef.get(), packet));
        signalWrite();
    }

    public void writeServer(ServerPacket packet) {
        if (packet == null || closing) return;
        writeQueue.add(new ServerQueuedPacket(writeStateRef.get(), packet));
        signalWrite();
    }

    public boolean flush(Consumer<NetworkBuffer> writer) {
        NetworkBuffer leftover = this.writeLeftover;
        if (leftover != null) {
            try {
                writer.accept(leftover);
            } catch (Exception e) {
                this.writeLeftover = null;
                PacketVanilla.PACKET_POOL.add(leftover);
                return false;
            }
            if (leftover.readableBytes() == 0) {
                this.writeLeftover = null;
                PacketVanilla.PACKET_POOL.add(leftover);
            } else {
                return true;
            }
        }

        if (writeQueue.isEmpty()) {
            writeLock.lock();
            try {
                while (writeQueue.isEmpty() && !closing) {
                    writeCondition.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                writeLock.unlock();
            }
            if (closing && writeQueue.isEmpty()) {
                return false;
            }
        }

        NetworkBuffer buffer = PacketVanilla.PACKET_POOL.get();

        PacketWriting.writeQueue(buffer, writeQueue, 1, (networkBuffer, queuedPacket) -> {
            try {
                writeQueuedPacket(networkBuffer, queuedPacket);
                return true;
            } catch (Exception e) {
                return false;
            }
        });

        try {
            writer.accept(buffer);
        } catch (Exception e) {
            PacketVanilla.PACKET_POOL.add(buffer);
            return false;
        }

        if (buffer.readableBytes() == 0) {
            PacketVanilla.PACKET_POOL.add(buffer);
        } else {
            this.writeLeftover = buffer;
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

    private static void writeQueuedPacket(NetworkBuffer buffer, QueuedPacket queuedPacket) {
        switch (queuedPacket) {
            case ClientQueuedPacket clientQueuedPacket -> PacketWriting.writeFramedPacket(
                    buffer,
                    clientQueuedPacket.state(),
                    clientQueuedPacket.packet(),
                    0
            );
            case ServerQueuedPacket serverQueuedPacket -> PacketWriting.writeFramedPacket(
                    buffer,
                    serverQueuedPacket.state(),
                    serverQueuedPacket.packet(),
                    0
            );
        }
    }

    private sealed interface QueuedPacket permits ClientQueuedPacket, ServerQueuedPacket {
        ConnectionState state();
    }

    private record ClientQueuedPacket(ConnectionState state, ClientPacket packet) implements QueuedPacket {
    }

    private record ServerQueuedPacket(ConnectionState state, ServerPacket packet) implements QueuedPacket {
    }
}