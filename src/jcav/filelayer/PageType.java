package jcav.filelayer;

public class PageType {
    public static final byte NULL = 0;
    public static final byte FREE = 1;
    public static final byte OVERFLOW = 2;
    public static final byte BTREE_NULL = 3;
    public static final byte BTREE_INTERIOR = 4;
    public static final byte BTREE_LEAF = 5;

    public static String to_string(int type) {
        switch(type) {
            case NULL:
                return "NULL";
            case FREE:
                return "FREE";
            case OVERFLOW:
                return "OVERFLOW";
            case BTREE_NULL:
                return "BTREE_NULL";
            case BTREE_INTERIOR:
                return "BTREE_INTERIOR";
            case BTREE_LEAF:
                return "BTREE_LEAF";
            default:
                return "UNKNOWN(" + type + ")";
        }
    }
}
