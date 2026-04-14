import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {
    public static final int BITS = 8;           // Size of the ID space
    public static final int RING_SIZE = 1 << BITS; // 2^BITS = 256

    // Hash a string (IP+port or filename) to a node ID in [0, RING_SIZE)
    public static int hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hashBytes = md.digest(input.getBytes());
            BigInteger hashInt = new BigInteger(1, hashBytes);
            return hashInt.mod(BigInteger.valueOf(RING_SIZE)).intValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    }

    // Check if id is in the range (start, end] on the ring, handling wraparound
    public static boolean inRangeInclusive(int id, int start, int end) {
        if (start < end) {
            return id > start && id <= end;
        } else { // wraparound
            return id > start || id <= end;
        }
    }

    // Check if id is in the range (start, end) on the ring, handling wraparound
    public static boolean inRangeExclusive(int id, int start, int end) {
        if (start < end) {
            return id > start && id < end;
        } else { // wraparound
            return id > start || id < end;
        }
    }
}