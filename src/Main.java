import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        if (args.length != 2 && args.length != 4) {
            System.out.println("Usage:");
            System.out.println("  Start a new ring : java Main <myIp> <myPort>");
            System.out.println("  Join a ring      : java Main <myIp> <myPort> <bootstrapIp> <bootstrapPort>");
            return;
        }

        String myIp = args[0];
        int myPort = Integer.parseInt(args[1]);

        // Start the node
        ChordNode node = new ChordNode(myIp, myPort);

        // Start the server in a background thread
        Thread serverThread = new Thread(new Server(node, myPort));
        serverThread.setDaemon(true);
        serverThread.start();

        // Either start a new ring or join an existing one
        if (args.length == 2) {
            node.startRing();
        } else {
            String bootstrapIp = args[2];
            int bootstrapPort = Integer.parseInt(args[3]);
            node.join(bootstrapIp, bootstrapPort);
        }

        // Schedule stabilize and fixFingers to run periodically
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(node::stabilize, 2, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(node::fixFingers, 3, 5, TimeUnit.SECONDS);

        // Interactive CLI prompt
        Scanner scanner = new Scanner(System.in);
        System.out.println("Commands: put <filename> <contents> | get <filename> | leave | print");
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split(" ", 3);
            switch (parts[0]) {
                case "put":
                    if (parts.length < 3) { System.out.println("Usage: put <filename> <contents>"); break; }
                    node.put(parts[1], parts[2]);
                    break;
                case "get":
                    if (parts.length < 2) { System.out.println("Usage: get <filename>"); break; }
                    String result = node.get(parts[1]);
                    System.out.println(result != null ? "=> " + result : "File not found");
                    break;
                case "leave":
                    node.leave();
                    scheduler.shutdown();
                    return;
                case "print":
                    node.printState();
                    break;
                default:
                    System.out.println("Unknown command. Commands: put | get | leave | print");
            }
        }
    }
}