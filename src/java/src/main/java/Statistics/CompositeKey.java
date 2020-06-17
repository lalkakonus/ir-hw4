package Statistics;

public class CompositeKey {

    public static String getKeyType(String compKey) { return compKey.split("_")[2].substring(1); }

    public static String getKey(String queryID, String urlID, Boolean isHost, String keyType) {
        return (queryID.charAt(0) == '-' ? "X" : queryID) + "_" +
               (urlID.charAt(0) == '-' ? "X" : urlID) + "_" +
               (isHost ? "H" : "U") + keyType;
    }
}