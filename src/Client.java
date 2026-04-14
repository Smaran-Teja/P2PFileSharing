import java.io.*;
import java.net.*;

public class Client {

    // Send a message to a target node and return the response
    public static Message send(NodeInfo target, Message message) {
        try (
                Socket socket = new Socket(target.ip, target.port);
                PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println(message.serialize());
            String response = in.readLine();
            return Message.deserialize(response);
        } catch (IOException e) {
            System.err.println("Failed to contact " + target + ": " + e.getMessage());
            return null;
        }
    }
}