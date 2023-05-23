package jcav.filelayer.btree;

public final class CellType {
    public static final byte FREE = 0;
    public static final byte INTERIOR = 1;
    public static final byte LEAF = 2;

    public static String to_string(byte type) {
        switch (type) {
            case FREE:
                return "FREE";
            case INTERIOR:
                return "INTERIOR";
            case LEAF:
                return "LEAF";
            default:
                return "UNKNOWN(" + type + ")";
        }
    }
}
