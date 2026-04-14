import java.util.HashMap;
import java.util.Map;

public class ChordNode {
    private final NodeInfo self;
    private NodeInfo successor;
    private NodeInfo predecessor;
    private final NodeInfo[] fingerTable = new NodeInfo[HashUtil.BITS];
    private final Map<String, String> store = new HashMap<>(); // filename -> contents

    public ChordNode(String ip, int port) {
        int id = HashUtil.hash(ip + ":" + port);
        this.self = new NodeInfo(id, ip, port);
    }

    // --- Ring initialization ---

    public void startRing() {
        successor = self;
        predecessor = self;
        System.out.println("Started new ring as " + self);
    }

    public void join(String bootstrapIp, int bootstrapPort) {
        int bootstrapId = HashUtil.hash(bootstrapIp + ":" + bootstrapPort);
        NodeInfo bootstrap = new NodeInfo(bootstrapId, bootstrapIp, bootstrapPort);
        predecessor = null;
        successor = findSuccessor(self.id, bootstrap);
        System.out.println(self + " joined ring, successor = " + successor);
    }

    // --- Core Chord routing ---

    // Find the successor of a given ID, starting the search from a given node
    private NodeInfo findSuccessor(int id, NodeInfo start) {
        Message response = Client.send(start, new Message(MessageType.FIND_SUCCESSOR, self, String.valueOf(id)));
        if (response == null) return null;
        return NodeInfo.deserialize(response.payload);
    }

    // Find the successor of a given ID locally, routing through the finger table
    public NodeInfo findSuccessorLocal(int id) {
        if (HashUtil.inRangeInclusive(id, self.id, successor.id)) {
            return successor;
        }
        NodeInfo closest = closestPrecedingNode(id);
        if (closest.id == self.id) return successor;
        return findSuccessor(id, closest);
    }

    // Find the closest preceding node for a given ID using the finger table
    private NodeInfo closestPrecedingNode(int id) {
        for (int i = HashUtil.BITS - 1; i >= 0; i--) {
            if (fingerTable[i] != null && HashUtil.inRangeExclusive(fingerTable[i].id, self.id, id)) {
                return fingerTable[i];
            }
        }
        return self;
    }

    // --- Stabilization ---

    public void stabilize() {
        if (successor.id == self.id) return;
        Message response = Client.send(successor, new Message(MessageType.GET_PREDECESSOR, self, ""));
        if (response == null || response.payload.isEmpty()) return;
        NodeInfo x = NodeInfo.deserialize(response.payload);
        if (HashUtil.inRangeExclusive(x.id, self.id, successor.id)) {
            successor = x;
        }
        Client.send(successor, new Message(MessageType.NOTIFY, self, ""));
    }

    public void notify(NodeInfo candidate) {
        if (predecessor == null || HashUtil.inRangeExclusive(candidate.id, predecessor.id, self.id)) {
            predecessor = candidate;
        }
    }

    public void fixFingers() {
        for (int i = 0; i < HashUtil.BITS; i++) {
            int start = (self.id + (1 << i)) % HashUtil.RING_SIZE;
            fingerTable[i] = findSuccessorLocal(start);
        }
    }

    // --- File operations ---

    public void put(String filename, String contents) {
        int key = HashUtil.hash(filename);
        NodeInfo target = findSuccessorLocal(key);
        if (target.id == self.id) {
            store.put(filename, contents);
            System.out.println("Stored '" + filename + "' locally");
        } else {
            Client.send(target, new Message(MessageType.PUT, self, filename + ":" + contents));
        }
    }

    public String get(String filename) {
        int key = HashUtil.hash(filename);
        NodeInfo target = findSuccessorLocal(key);
        if (target.id == self.id) {
            return store.getOrDefault(filename, null);
        } else {
            Message response = Client.send(target, new Message(MessageType.GET, self, filename));
            return (response != null) ? response.payload : null;
        }
    }

    // --- Graceful leave ---

    public void leave() {
        // Transfer all stored files to successor
        for (Map.Entry<String, String> entry : store.entrySet()) {
            Client.send(successor, new Message(MessageType.PUT, self, entry.getKey() + ":" + entry.getValue()));
        }
        store.clear();
        System.out.println(self + " has left the ring");
    }

    // --- Incoming message handler (called by Server) ---

    public Message handleMessage(Message msg) {
        switch (msg.type) {
            case FIND_SUCCESSOR: {
                int id = Integer.parseInt(msg.payload);
                NodeInfo result = findSuccessorLocal(id);
                return new Message(MessageType.REPLY, self, result.serialize());
            }
            case GET_PREDECESSOR: {
                String payload = (predecessor != null) ? predecessor.serialize() : "";
                return new Message(MessageType.REPLY, self, payload);
            }
            case GET_SUCCESSOR: {
                return new Message(MessageType.REPLY, self, successor.serialize());
            }
            case NOTIFY: {
                notify(msg.sender);
                return new Message(MessageType.REPLY, self, "");
            }
            case PUT: {
                String[] parts = msg.payload.split(":", 2);
                store.put(parts[0], parts[1]);
                return new Message(MessageType.REPLY, self, "OK");
            }
            case GET: {
                String contents = store.getOrDefault(msg.payload, "");
                return new Message(MessageType.REPLY, self, contents);
            }
            default:
                return new Message(MessageType.REPLY, self, "UNKNOWN");
        }
    }

    // --- Utility ---

    public void printState() {
        System.out.println("Self:        " + self);
        System.out.println("Successor:   " + successor);
        System.out.println("Predecessor: " + predecessor);
        System.out.println("Finger table:");
        for (int i = 0; i < HashUtil.BITS; i++) {
            System.out.println("  [" + i + "] start=" + ((self.id + (1 << i)) % HashUtil.RING_SIZE) + " -> " + fingerTable[i]);
        }
        System.out.println("Files: " + store.keySet());
    }

    public NodeInfo getSelf() { return self; }
}