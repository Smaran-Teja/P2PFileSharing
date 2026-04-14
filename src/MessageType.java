
public enum MessageType {
    // Ring maintenance
    FIND_SUCCESSOR,     // Ask a node to find the successor of a given ID
    GET_SUCCESSOR,      // Ask a node for its current successor
    GET_PREDECESSOR,    // Ask a node for its current predecessor
    NOTIFY,             // Inform a node that it may need to update its predecessor

    // File operations
    PUT,                // Store a file in the network
    GET,                // Retrieve a file from the network

    // Response
    REPLY               // Generic response to any of the above
}