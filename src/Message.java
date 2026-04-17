public class Message {
    public final MessageType type;
    public final NodeInfo sender;
    public final String payload;
    public final int hopCount;

    public Message(MessageType type, NodeInfo sender, String payload) {
        this(type, sender, payload, 0);
    }

    public Message(MessageType type, NodeInfo sender, String payload, int hopCount) {
        this.type = type;
        this.sender = sender;
        this.payload = payload;
        this.hopCount = hopCount;
    }

    // Format: "TYPE|id,ip,port|hopCount|payload"
    public String serialize() {
        return type.name() + "|" + sender.serialize() + "|" + hopCount + "|" + payload;
    }

    public static Message deserialize(String s) {
        String[] parts = s.split("\\|", 4);
        MessageType type = MessageType.valueOf(parts[0]);
        NodeInfo sender = NodeInfo.deserialize(parts[1]);
        int hopCount = Integer.parseInt(parts[2]);
        String payload = parts[3];
        return new Message(type, sender, payload, hopCount);
    }

    @Override
    public String toString() {
        return "Message(" + type + " from " + sender + " hops=" + hopCount + " payload='" + payload + "')";
    }
}