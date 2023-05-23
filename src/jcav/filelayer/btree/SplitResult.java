package jcav.filelayer.btree;

public class SplitResult {
    public BTreeNode left;
    public BTreeNode right;
    public Payload key;
    public int root_page_id;
}
