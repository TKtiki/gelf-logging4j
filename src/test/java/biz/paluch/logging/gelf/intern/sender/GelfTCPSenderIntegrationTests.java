package biz.paluch.logging.gelf.intern.sender;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import biz.paluch.logging.gelf.intern.ErrorReporter;
import biz.paluch.logging.gelf.intern.GelfMessage;

/**
 * @author <a href="mailto:mpaluch@paluch.biz">Mark Paluch</a>
 */
class GelfTCPSenderIntegrationTests {

    private static final int PORT = 1234;

    private final Queue<Socket> sockets = new LinkedBlockingQueue<>();
    private volatile ServerSocket serverSocket;
    private volatile boolean loopActive = true;
    private volatile boolean readFromServerSocket = true;

    private CompletableFuture<ByteArrayOutputStream> serverFuture;

    @BeforeEach
    void setUp() throws Exception {
        serverSocket = new ServerSocket(PORT);
        serverSocket.setSoTimeout(10000);
    }


    private CompletableFuture<ByteArrayOutputStream> startServer() {
        return CompletableFuture.supplyAsync(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try {
                while (loopActive) {
                    if (serverSocket.isClosed()) {
                        break;
                    }

                    Socket socket = serverSocket.accept();
                    sockets.add(socket);
                    socket.setKeepAlive(true);

                    InputStream inputStream = socket.getInputStream();

                    if (readFromServerSocket) {
                        IOUtils.copy(inputStream, out);
                    } else {
                        while (!socket.isClosed() && loopActive) {
                            Thread.sleep(50);
                        }
                    }

                    socket.close();
                }
            } catch (IOException ignored) {
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return out;
        });
    }



    @AfterEach
    void tearDown() throws IOException {
        loopActive = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }

        if (serverFuture != null) {
            serverFuture.cancel(true);
        }
    }

    @Test
    void simpleTransport() throws Exception {
        serverFuture = startServer();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (message, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");
        ByteBuffer byteBuffer = gelfMessage.toTCPBuffer();
        int size = byteBuffer.remaining();

        sender.sendMessage(gelfMessage);
        sender.close();

        loopActive = false;
        ByteArrayOutputStream out = serverFuture.join();

        assertThat(out.size()).isEqualTo(size);
    }

    @Test
    void shouldRecoverFromBrokenPipe() throws Exception {
        serverFuture = startServer();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (message, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sockets.poll().close();

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sender.close();
    }

    @Test
    void shouldRecoverFromClosedPort() throws Exception {
        serverFuture = startServer();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (message, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sockets.poll().close();
        serverSocket.close();

        assertThat(sender.sendMessage(gelfMessage)).isFalse();

        serverSocket = new ServerSocket(PORT);
        serverFuture = startServer();

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sender.close();
    }


    @Test
    void sendToNonConsumingPort() throws Exception {
        serverSocket.setReceiveBufferSize(100);
        readFromServerSocket = false;
        serverFuture = startServer();

        final List<String> errors = new ArrayList<>();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (message, e) -> {
            errors.add(message);
        });

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");

        sender.sendMessage(gelfMessage);

        assertThat(errors).hasSize(1);
        assertThat(errors).containsOnly("Cannot write buffer to channel, no progress in writing");

        sender.close();
    }

    static class SmallBufferTCPSender extends GelfTCPSender {
        SmallBufferTCPSender(String host, int port, int connectTimeoutMs, int readTimeoutMs, ErrorReporter errorReporter)
                throws IOException {
            super(host, port, connectTimeoutMs, readTimeoutMs, errorReporter);
        }

        @Override
        protected SocketChannel createSocketChannel(int readTimeoutMs, boolean keepAlive) throws IOException {
            SocketChannel socketChannel = super.createSocketChannel(readTimeoutMs, keepAlive);
            socketChannel.socket().setSendBufferSize(100);
            return socketChannel;
        }
    }
}
