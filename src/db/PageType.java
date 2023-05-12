package db;

public class PageType {
    public static final byte NULL = 0;
    public static final byte FREE = 1;
    public static final byte OVERFLOW = 2;
    public static final byte BTREE_NULL = 3;
    public static final byte BTREE_INTERIOR = 4;
    public static final byte BTREE_LEAF = 5;
}
