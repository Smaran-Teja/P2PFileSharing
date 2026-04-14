import java.io.*;
import java.net.*;

public class Server implements Runnable {
    private final ChordNode node;
    private final int port;

    public Server(ChordNode node, int port) {
        this.node = node;
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server listening on port " + port);
            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() -> handle(client)).start();
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private void handle(Socket client) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream()), true)
        ) {
            String line = in.readLine();
            Message message = Message.deserialize(line);
            Message response = node.handleMessage(message);
            out.println(response.serialize());
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        }
    }
}