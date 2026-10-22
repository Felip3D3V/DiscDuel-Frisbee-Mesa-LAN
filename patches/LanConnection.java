package br.com.discduel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Comunicação TCP com entrada e saída em threads de trabalho.
 * IMPORTANTE: send() é não-bloqueante e seguro para chamadas vindas da View.
 * O Android não permite flush()/write() em socket na thread principal.
 */
public final class LanConnection {
    public static final int PORT = 45328;

    public interface Listener {
        void onConnected();
        void onMessage(String message);
        void onDisconnected(String reason);
    }

    private final Listener listener;
    private final BlockingQueue<String> outgoing = new ArrayBlockingQueue<>(32);
    private final AtomicBoolean notified = new AtomicBoolean(false);
    private volatile boolean running;
    private volatile boolean connected;
    private volatile ServerSocket server;
    private volatile Socket socket;
    private volatile Thread writerThread;

    public LanConnection(Listener listener) {
        this.listener = listener;
    }

    public boolean isConnected() { return running && connected; }

    public void host() {
        running = true;
        notified.set(false);
        Thread t = new Thread(() -> {
            try {
                ServerSocket listening = new ServerSocket(PORT);
                server = listening;
                if (!running) { listening.close(); return; }
                Socket accepted = listening.accept();
                if (running) startSession(accepted);
                else accepted.close();
            } catch (IOException e) {
                signalDisconnect("Não foi possível criar a sala: " + e.getMessage());
            } finally {
                close();
            }
        }, "DiscDuel-Server");
        t.start();
    }

    public void join(String ip) {
        running = true;
        notified.set(false);
        Thread t = new Thread(() -> {
            try {
                Socket client = new Socket();
                socket = client;
                client.connect(new InetSocketAddress(ip, PORT), 6500);
                if (running) startSession(client);
                else client.close();
            } catch (IOException e) {
                signalDisconnect("Falha na conexão: " + e.getMessage());
            } finally {
                close();
            }
        }, "DiscDuel-Client");
        t.start();
    }

    private void startSession(Socket peer) {
        try {
            socket = peer;
            peer.setTcpNoDelay(true);
            peer.setKeepAlive(true);
            BufferedWriter output = new BufferedWriter(new OutputStreamWriter(
                    peer.getOutputStream(), StandardCharsets.UTF_8));
            BufferedReader input = new BufferedReader(new InputStreamReader(
                    peer.getInputStream(), StandardCharsets.UTF_8));
            outgoing.clear();
            connected = true;

            // Nunca escreva no socket a partir de onDraw()/onTouchEvent().
            Thread sender = new Thread(() -> writeLoop(output), "DiscDuel-Network-Writer");
            writerThread = sender;
            sender.start();
            listener.onConnected();

            String line;
            while (running && (line = input.readLine()) != null) {
                if (line.length() <= 512) listener.onMessage(line);
            }
            signalDisconnect("O outro jogador saiu da partida.");
        } catch (IOException e) {
            signalDisconnect("Conexão interrompida: " + e.getMessage());
        } finally {
            close();
        }
    }

    private void writeLoop(BufferedWriter output) {
        try {
            while (running && connected) {
                String message = outgoing.poll(250, TimeUnit.MILLISECONDS);
                if (message == null) continue;
                output.write(message);
                output.newLine();
                output.flush();
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            signalDisconnect("Falha ao enviar dados: " + e.getMessage());
            close();
        }
    }

    /** Enfileira sem acessar o socket e sem bloquear a thread da interface. */
    public boolean send(String line) {
        if (!running || !connected || line == null || line.length() > 512) return false;
        if (outgoing.offer(line)) return true;
        // O estado mais recente importa mais que quadros antigos: evitar filas crescentes.
        outgoing.poll();
        return outgoing.offer(line);
    }

    private void signalDisconnect(String message) {
        if (running && notified.compareAndSet(false, true)) {
            listener.onDisconnected(message);
        }
    }

    public void close() {
        running = false;
        connected = false;
        outgoing.clear();
        Thread sender = writerThread;
        writerThread = null;
        if (sender != null && sender != Thread.currentThread()) sender.interrupt();
        try { Socket peer = socket; if (peer != null) peer.close(); } catch (IOException ignored) { }
        try { ServerSocket listening = server; if (listening != null) listening.close(); } catch (IOException ignored) { }
    }
}
