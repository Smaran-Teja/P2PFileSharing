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
        successorList[0] = self;
        System.out.println("Started new ring as " + self);
    }

    public void join(String bootstrapIp, int bootstrapPort) {
        int bootstrapId = HashUtil.hash(bootstrapIp + ":" + bootstrapPort);
        NodeInfo bootstrap = new NodeInfo(bootstrapId, bootstrapIp, bootstrapPort);
        predecessor = null;
        // Use raw send for join — hop count not needed
        Message r = Client.send(bootstrap, new Message(MessageType.FIND_SUCCESSOR, self, String.valueOf(self.id), 0));
        successor = (r != null) ? NodeInfo.deserialize(r.payload) : self;
        successorList[0] = successor;
        System.out.println(self + " joined, successor = " + successor);
    }

    // --- Core routing ---

    // Used by stabilize/fixFingers — no hop tracking, returns just the node
    public NodeInfo findSuccessorLocal(int id) {
        if (HashUtil.inRangeInclusive(id, self.id, successor.id)) return successor;
        NodeInfo closest = closestPrecedingNode(id);
        if (closest.id == self.id) return successor;
        Message r = Client.send(closest, new Message(MessageType.FIND_SUCCESSOR, self, String.valueOf(id), 0));
        if (r == null) { clearFingerEntry(closest.id); return successor; }
        return NodeInfo.deserialize(r.payload);
    }

    // Used by handleMessage — resolves locally or forwards, always echoing back final hopCount
    private Message findSuccessorAndReply(int id, int hopCount) {
        if (HashUtil.inRangeInclusive(id, self.id, successor.id))
            return replyWithHops(successor.serialize(), hopCount);
        NodeInfo closest = closestPrecedingNode(id);
        if (closest.id == self.id) return replyWithHops(successor.serialize(), hopCount);
        // Forward to next node with incremented hop count
        Message r = Client.send(closest, new Message(MessageType.FIND_SUCCESSOR, self, String.valueOf(id), hopCount + 1));
        if (r == null) { clearFingerEntry(closest.id); return replyWithHops(successor.serialize(), hopCount); }
        // Echo back the final hopCount from the deeper reply
        return replyWithHops(r.payload, r.hopCount);
    }

    // Used by put/get — sends FIND_SUCCESSOR and reads total hops from reply
    private NodeInfo findSuccessorTracked(int id) {
        if (HashUtil.inRangeInclusive(id, self.id, successor.id)) {
            System.out.println("  resolved in 0 hop(s)");
            return successor;
        }
        NodeInfo closest = closestPrecedingNode(id);
        if (closest.id == self.id) {
            System.out.println("  resolved in 0 hop(s)");
            return successor;
        }
        Message r = Client.send(closest, new Message(MessageType.FIND_SUCCESSOR, self, String.valueOf(id), 1));
        if (r == null) { clearFingerEntry(closest.id); return successor; }
        System.out.println("  resolved in " + r.hopCount + " hop(s)");
        return NodeInfo.deserialize(r.payload);
    }

    private NodeInfo closestPrecedingNode(int id) {
        for (int i = HashUtil.BITS - 1; i >= 0; i--) {
            if (fingerTable[i] != null && HashUtil.inRangeExclusive(fingerTable[i].id, self.id, id))
                return fingerTable[i];
        }
        return self;
    }

    private void clearFingerEntry(int id) {
        for (int i = 0; i < HashUtil.BITS; i++) {
            if (fingerTable[i] != null && fingerTable[i].id == id)
                fingerTable[i] = null;
        }
    }

    // --- Stabilization ---

    public void stabilize() {
        Message response = Client.send(successor, new Message(MessageType.GET_PREDECESSOR, self, ""));
        if (response == null) {
            NodeInfo alive = nextAliveSuccessor();
            if (alive.id != self.id) {
                System.out.println("Successor " + successor + " unreachable, failing over to " + alive);
                successor = alive;
                successorList[0] = successor;
            }
            return;
        }
        if (!response.payload.isEmpty()) {
            NodeInfo x = NodeInfo.deserialize(response.payload);
            if (x.id != self.id && HashUtil.inRangeExclusive(x.id, self.id, successor.id)) {
                Message check = Client.send(x, new Message(MessageType.GET_PREDECESSOR, self, ""));
                if (check != null) successor = x;
            }
        }
        Client.send(successor, new Message(MessageType.NOTIFY, self, ""));

        successorList[0] = successor;
        Message sl = Client.send(successor, new Message(MessageType.GET_SUCCESSOR_LIST, self, ""));
        if (sl != null && !sl.payload.isEmpty()) {
            String[] parts = sl.payload.split(";");
            for (int i = 0; i < SUCCESSOR_LIST_SIZE - 1 && i < parts.length; i++)
                successorList[i + 1] = NodeInfo.deserialize(parts[i]);
        }
    }

    public void notify(NodeInfo candidate) {
        if (predecessor == null || predecessor.id == self.id) { predecessor = candidate; return; }
        if (HashUtil.inRangeExclusive(candidate.id, predecessor.id, self.id)) { predecessor = candidate; return; }
        Message check = Client.send(predecessor, new Message(MessageType.GET_PREDECESSOR, self, ""));
        if (check == null) predecessor = candidate;
    }

    private NodeInfo nextAliveSuccessor() {
        for (NodeInfo c : successorList) {
            if (c == null || c.id == self.id) continue;
            Message r = Client.send(c, new Message(MessageType.GET_PREDECESSOR, self, ""));
            if (r != null) return c;
        }
        return self;
    }

    public void fixFingers() {
        for (int i = 0; i < HashUtil.BITS; i++) {
            int start = (self.id + (1 << i)) % HashUtil.RING_SIZE;
            NodeInfo result = findSuccessorLocal(start);
            if (result != null) fingerTable[i] = result;
        }
    }

    // --- File operations ---

    public void put(String filename, String contents) {
        int key = HashUtil.hash(filename);
        System.out.println("put '" + filename + "' (key=" + key + ")");
        NodeInfo target = findSuccessorTracked(key);
        if (target.id == self.id) {
            store.put(filename, contents);
            System.out.println("  stored locally");
        } else {
            System.out.println("  routing to " + target);
            Client.send(target, new Message(MessageType.PUT, self, filename + ":" + contents));
        }
    }

    public String get(String filename) {
        int key = HashUtil.hash(filename);
        System.out.println("get '" + filename + "' (key=" + key + ")");
        NodeInfo target = findSuccessorTracked(key);
        if (target.id == self.id) return store.getOrDefault(filename, null);
        System.out.println("  routing to " + target);
        Message r = Client.send(target, new Message(MessageType.GET, self, filename));
        return (r != null) ? r.payload : null;
    }

    // --- Graceful leave ---

    public void leave() {
        for (Map.Entry<String, String> e : store.entrySet())
            Client.send(successor, new Message(MessageType.PUT, self, e.getKey() + ":" + e.getValue()));
        store.clear();
        if (predecessor != null && predecessor.id != self.id)
            Client.send(predecessor, new Message(MessageType.UPDATE_SUCCESSOR, self, successor.serialize()));
        if (successor != null && successor.id != self.id)
            Client.send(successor, new Message(MessageType.UPDATE_PREDECESSOR, self,
                    predecessor != null ? predecessor.serialize() : ""));
        System.out.println(self + " has left the ring");
    }

    // --- Incoming message handler ---

    public Message handleMessage(Message msg) {
        switch (msg.type) {
            case FIND_SUCCESSOR:
                return findSuccessorAndReply(Integer.parseInt(msg.payload), msg.hopCount);
            case GET_PREDECESSOR:
                return reply(predecessor != null ? predecessor.serialize() : "");
            case GET_SUCCESSOR:
                return reply(successor.serialize());
            case NOTIFY:
                notify(msg.sender);
                return reply("");
            case GET_SUCCESSOR_LIST: {
                StringBuilder sb = new StringBuilder();
                for (NodeInfo n : successorList) {
                    if (n != null) { if (sb.length() > 0) sb.append(";"); sb.append(n.serialize()); }
                }
                return reply(sb.toString());
            }
            case UPDATE_SUCCESSOR:
                successor = NodeInfo.deserialize(msg.payload);
                successorList[0] = successor;
                System.out.println("Successor updated to " + successor);
                return reply("OK");
            case UPDATE_PREDECESSOR:
                if (!msg.payload.isEmpty()) {
                    predecessor = NodeInfo.deserialize(msg.payload);
                    System.out.println("Predecessor updated to " + predecessor);
                }
                return reply("OK");
            case PUT: {
                String[] parts = msg.payload.split(":", 2);
                store.put(parts[0], parts[1]);
                return reply("OK");
            }
            case GET:
                return reply(store.getOrDefault(msg.payload, ""));
            default:
                return reply("UNKNOWN");
        }
    }

    private Message reply(String payload) {
        return new Message(MessageType.REPLY, self, payload);
    }

    private Message replyWithHops(String payload, int hopCount) {
        return new Message(MessageType.REPLY, self, payload, hopCount);
    }

    // --- Utility ---

    public void printState() {
        System.out.println("Self:        " + self);
        System.out.println("Successor:   " + successor);
        System.out.println("Succ list:");
        for (int i = 0; i < SUCCESSOR_LIST_SIZE; i++)
            System.out.println("  [" + i + "] " + successorList[i]);
        System.out.println("Predecessor: " + predecessor);
        System.out.println("Finger table:");
        for (int i = 0; i < HashUtil.BITS; i++)
            System.out.println("  [" + i + "] start=" + ((self.id + (1 << i)) % HashUtil.RING_SIZE) + " -> " + fingerTable[i]);
        System.out.println("Files: " + store.keySet());
    }

    public NodeInfo getSelf() { return self; }
}