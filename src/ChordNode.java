import java.util.HashMap;
import java.util.Map;

public class ChordNode {
    private final NodeInfo self;
    private NodeInfo successor;
    private NodeInfo predecessor;
    private final NodeInfo[] fingerTable = new NodeInfo[HashUtil.BITS];
    private final Map<String, String> store = new HashMap<>();

    private static final int SUCCESSOR_LIST_SIZE = 3;
    private final NodeInfo[] successorList = new NodeInfo[SUCCESSOR_LIST_SIZE];

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
        NodeInfo result = findSuccessor(id, closest);
        return (result != null) ? result : successor; // fall back if closest is unreachable
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
        Message response = Client.send(successor, new Message(MessageType.GET_PREDECESSOR, self, ""));
        if (response == null) {
            // Successor is unreachable — fall back to next alive in successor list
            NodeInfo alive = nextAliveSuccessor();
            if (alive != null && alive.id != self.id) {
                System.out.println("Successor " + successor + " unreachable, failing over to " + alive);
                successor = alive;
            }
            return;
        }
        if (!response.payload.isEmpty()) {
            NodeInfo x = NodeInfo.deserialize(response.payload);
            if (HashUtil.inRangeExclusive(x.id, self.id, successor.id)) {
                successor = x;
            }
        }
        Client.send(successor, new Message(MessageType.NOTIFY, self, ""));

        // Refresh successor list from our successor's list
        Message slResponse = Client.send(successor, new Message(MessageType.GET_SUCCESSOR_LIST, self, ""));
        if (slResponse != null && !slResponse.payload.isEmpty()) {
            successorList[0] = successor;
            String[] parts = slResponse.payload.split(";");
            for (int i = 0; i < SUCCESSOR_LIST_SIZE - 1 && i < parts.length; i++) {
                successorList[i + 1] = NodeInfo.deserialize(parts[i]);
            }
        }
    }

    // Walk the successor list to find the first reachable node
    private NodeInfo nextAliveSuccessor() {
        for (NodeInfo candidate : successorList) {
            if (candidate == null || candidate.id == self.id) continue;
            Message response = Client.send(candidate, new Message(MessageType.GET_PREDECESSOR, self, ""));
            if (response != null) return candidate;
        }
        return self;
    }

    public void notify(NodeInfo candidate) {
        if (predecessor == null || HashUtil.inRangeExclusive(candidate.id, predecessor.id, self.id)) {
            predecessor = candidate;
        }
    }

    public void fixFingers() {
        for (int i = 0; i < HashUtil.BITS; i++) {
            int start = (self.id + (1 << i)) % HashUtil.RING_SIZE;
            NodeInfo result = findSuccessorLocal(start);
            if (result != null) fingerTable[i] = result; // skip update if unreachable
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
        // Tell predecessor to skip over this node to our successor
        if (predecessor != null && predecessor.id != self.id) {
            Client.send(predecessor, new Message(MessageType.UPDATE_SUCCESSOR, self, successor.serialize()));
        }
        // Tell successor to update its predecessor to our predecessor
        if (successor != null && successor.id != self.id) {
            Client.send(successor, new Message(MessageType.UPDATE_PREDECESSOR, self, predecessor.serialize()));
        }
        System.out.println(self + " has left the ring");
    }

    // --- Incoming message handler (called by Server) ---

    public Message handleMessage(Message msg) {
        switch (msg.type) {
            case GET_SUCCESSOR_LIST: {
                StringBuilder sb = new StringBuilder();
                for (NodeInfo n : successorList) {
                    if (n != null) {
                        if (sb.length() > 0) sb.append(";");
                        sb.append(n.serialize());
                    }
                }
                return new Message(MessageType.REPLY, self, sb.toString());
            }
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
            case UPDATE_PREDECESSOR: {
                predecessor = NodeInfo.deserialize(msg.payload);
                System.out.println("Predecessor updated to " + predecessor);
                return new Message(MessageType.REPLY, self, "OK");
            }
            case UPDATE_SUCCESSOR: {
                successor = NodeInfo.deserialize(msg.payload);
                System.out.println("Successor updated to " + successor);
                return new Message(MessageType.REPLY, self, "OK");
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