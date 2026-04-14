public class Message {
    public final MessageType type;
    public final NodeInfo sender;
    public final String payload; // key, value, serialized NodeInfo, or empty

    public Message(MessageType type, NodeInfo sender, String payload) {
        this.type = type;
        this.sender = sender;
        this.payload = payload;
    }

    // Serialize to a single line for transmission over the socket
    // Format: "TYPE|id,ip,port|payload"
    public String serialize() {
        return type.name() + "|" + sender.serialize() + "|" + payload;
    }

    // Deserialize from a line received over the socket
    public static Message deserialize(String s) {
        String[] parts = s.split("\\|", 3);
        MessageType type = MessageType.valueOf(parts[0]);
        NodeInfo sender = NodeInfo.deserialize(parts[1]);
        String payload = parts[2];
        return new Message(type, sender, payload);
    }

    @Override
    public String toString() {
        return "Message(" + type + " from " + sender + " payload='" + payload + "')";
    }
}