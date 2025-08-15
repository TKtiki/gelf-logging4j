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
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import biz.paluch.logging.gelf.intern.ErrorReporter;
import biz.paluch.logging.gelf.intern.GelfMessage;

class GelfTCPSenderIntegrationTests {

    private static final int PORT = 1234;
    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    private final Queue<Socket> sockets = new LinkedBlockingQueue<>();
    private volatile ServerSocket serverSocket;
    private volatile boolean loopActive = true;
    private volatile boolean readFromServerSocket = true;

    private Thread thread;
    private CountDownLatch serverReady;

    @BeforeEach
    void setUp() throws Exception {

        serverSocket = new ServerSocket(PORT);
        serverSocket.setSoTimeout(10000);
        serverReady = new CountDownLatch(1);

        thread = new Thread("GelfTCPSenderIntegrationTest-server") {

            @Override
            public void run() {
                loopActive = true;
                try {
                    serverReady.countDown(); // Server ist bereit
                    while (loopActive) {
                        Socket socket = serverSocket.accept();
                        sockets.add(socket);
                        socket.setKeepAlive(true);
                        InputStream inputStream = socket.getInputStream();

                        byte[] buffer = new byte[1024];
                        while (!socket.isClosed()) {
                            if (readFromServerSocket) {
                                int read = inputStream.read(buffer);
                                if (read == -1) {
                                    break;
                                }
                                out.write(buffer, 0, read);
                            } else {
                                Thread.sleep(1);
                            }
                        }
                    }
                } catch (IOException | InterruptedException ignored) {
                }
            }
        };
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException {
        loopActive = false;
        thread.interrupt();
        if (!serverSocket.isClosed()) {
            serverSocket.close();
        }
        thread.join(2000);
        for (Socket socket : sockets) {
            if (!socket.isClosed()) {
                socket.close();
            }
        }
    }

    @Test
    void simpleTransport() throws Exception {

        thread.start();
        serverReady.await(2, TimeUnit.SECONDS);

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (m, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");
        ByteBuffer byteBuffer = gelfMessage.toTCPBuffer();
        int size = byteBuffer.remaining();

        sender.sendMessage(gelfMessage);
        sender.close();

        loopActive = false;
        thread.join(2000);

        assertThat(out.size()).isEqualTo(size);
    }

    @Test
    void shouldRecoverFromBrokenPipe() throws Exception {

        thread.start();
        serverReady.await(2, TimeUnit.SECONDS);

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (m, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        Socket s = sockets.poll();
        s.close();
        Thread.sleep(50); // sicherstellen, dass Socket wirklich geschlossen ist

        assertThat(sender.sendMessage(gelfMessage)).isTrue();
        sender.close();
    }

    @Test
    void shouldRecoverFromClosedPort() throws Exception {

        thread.start();
        serverReady.await(2, TimeUnit.SECONDS);

        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (m, e) -> {});

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");

        assertThat(sender.sendMessage(gelfMessage)).isTrue();

        Socket s = sockets.poll();
        s.close();
        serverSocket.close();
        Thread.sleep(50);

        assertThat(sender.sendMessage(gelfMessage)).isFalse();

        serverSocket = new ServerSocket(PORT);
        thread = new Thread(thread.getName()) {
            @Override
            public void run() { /* leer, Server bereits neu gestartet */ }
        };
        thread.start();
        serverReady.await(2, TimeUnit.SECONDS);

        assertThat(sender.sendMessage(gelfMessage)).isTrue();
        sender.close();
    }

    @Test
    void sendToNonConsumingPort() throws Exception {

        serverSocket.setReceiveBufferSize(100);
        readFromServerSocket = false; // emulate read delays
        thread.start();
        serverReady.await(2, TimeUnit.SECONDS);

        final List<String> errors = new ArrayList<>();
        SmallBufferTCPSender sender = new SmallBufferTCPSender("localhost", PORT, 1000, 1000, (m, e) -> errors.add(m));

        GelfMessage gelfMessage = new GelfMessage("hello", StringUtils.repeat("hello", 100000), PORT, "7");
        sender.sendMessage(gelfMessage);

        // warten bis Error auftaucht
        long start = System.currentTimeMillis();
        while (errors.isEmpty() && (System.currentTimeMillis() - start < 2000)) {
            Thread.sleep(10);
        }

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
