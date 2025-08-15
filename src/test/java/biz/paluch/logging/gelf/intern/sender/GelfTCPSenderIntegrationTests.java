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
import java.util.concurrent.CountDownLatch;
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

    private ByteArrayOutputStream out;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Queue<Socket> sockets = new LinkedBlockingQueue<>();
    private volatile ServerSocket serverSocket;
    private volatile boolean loopActive;
    private volatile boolean readFromServerSocket;
    private Thread serverThread;

    private int port;

    @BeforeEach
    void setUp() throws Exception {
        out = new ByteArrayOutputStream();
        loopActive = true;
        readFromServerSocket = true;

        // Dynamischer Port für CI-Umgebungen
        serverSocket = new ServerSocket(0);
        serverSocket.setSoTimeout(5000);
        port = serverSocket.getLocalPort();

        serverThread = new Thread(() -> {
            while (loopActive) {
                try {
                    Socket socket = serverSocket.accept();
                    sockets.add(socket);
                    socket.setKeepAlive(true);
                    try (InputStream inputStream = socket.getInputStream()) {
                        while (!socket.isClosed() && loopActive) {
                            if (readFromServerSocket) {
                                IOUtils.copy(inputStream, out);
                            }
                            Thread.sleep(1);
                            if (latch.getCount() == 0) {
                                socket.close();
                            }
                        }
                    }
                } catch (IOException | InterruptedException ignored) {
                }
            }
        }, "GelfTCPSenderIntegrationTest-server");
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException {
        loopActive = false;
        latch.countDown();
        if (serverThread != null) {
            serverThread.join(2000); // kein endloses Warten
        }
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        for (Socket s : sockets) {
            if (!s.isClosed()) {
                s.close();
            }
        }
    }

    @Test
    void simpleTransport() throws Exception {

        serverThread.start();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", port, 1000, 1000, new ErrorReporter() {
            @Override
            public void reportError(String message, Exception e) {
            }
        });

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), port, "7");
        ByteBuffer byteBuffer = gelfMessage.toTCPBuffer();
        int size = byteBuffer.remaining();

        sender.sendMessage(gelfMessage);
        sender.close();

        loopActive = false;
        latch.countDown();

        serverThread.join();

        assertThat(out.size()).isEqualTo(size);
    }

    @Test
    void shouldRecoverFromBrokenPipe() throws Exception {

        serverThread.start();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", port, 1000, 1000, new ErrorReporter() {
            @Override
            public void reportError(String message, Exception e) {
            }
        });

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), port, "7");
        ByteBuffer byteBuffer = gelfMessage.toTCPBuffer();

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sockets.poll().close();

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sender.close();
    }

    @Test
    void shouldRecoverFromClosedPort() throws Exception {

        serverThread.start();

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", port, 1000, 1000, new ErrorReporter() {
            @Override
            public void reportError(String message, Exception e) {
            }
        });

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), port, "7");
        ByteBuffer byteBuffer = gelfMessage.toTCPBuffer();

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sockets.poll().close();
        serverSocket.close();

        assertThat(sender.sendMessage(gelfMessage)).isFalse();

        serverSocket = new ServerSocket(port);

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        sender.close();
    }

    @Test
    void sendToNonConsumingPort() throws Exception {

        serverSocket.setReceiveBufferSize(50); // kleiner Buffer
        readFromServerSocket = false; // Server liest nichts
        serverThread.start();

        final List<String> errors = new ArrayList<>();
        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", port, 1000, 1000, (msg, e) -> errors.add(msg));

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), port, "7");

        sender.sendMessage(gelfMessage);

        // Kurzes Warten, damit der ErrorReporter auf GitHub Actions greifen kann
        Thread.sleep(200);

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