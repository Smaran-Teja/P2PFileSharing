public enum MessageType {
    // Ring maintenance
    FIND_SUCCESSOR,     // Ask a node to find the successor of a given ID
    GET_SUCCESSOR,      // Ask a node for its current successor
    GET_PREDECESSOR,    // Ask a node for its current predecessor
    NOTIFY,             // Inform a node that it may need to update its predecessor

    // File operations
    PUT,                // Store a file in the network
    GET,                // Retrieve a file from the network

    // Node departure
    UPDATE_SUCCESSOR,   // Tell a node to update its successor pointer
    UPDATE_PREDECESSOR, // Tell a node to update its predecessor pointer

    GET_SUCCESSOR_LIST, // Ask a node for its successor list
    REPLY               // Generic response to any of the above
}