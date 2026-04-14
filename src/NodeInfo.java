public class NodeInfo {
    public final int id;
    public final String ip;
    public final int port;

    public NodeInfo(int id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    public String serialize() {
        return id + "," + ip + "," + port;
    }

    public static NodeInfo deserialize(String s) {
        String[] parts = s.split(",");
        return new NodeInfo(Integer.parseInt(parts[0]), parts[1], Integer.parseInt(parts[2]));
    }

    public String toString() {
        return "Node(" + id + " @ " + ip + ":" + port + ")";
    }
}
