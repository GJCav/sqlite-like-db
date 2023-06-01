package jcav.filelayer.btree;

import jcav.filelayer.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.List;

public class SearchResult {
    /**
     * the path from root to the leaf. the last element is b-tree leaf. but the object type is {@link BTreeNode}.
     */
    public List<BTreeNode> path = new ArrayList<>();
    public List<Integer> idxs = new ArrayList<>();

    /**
     * if found the kay, idx is the slot_id of the key in its leaf node.
     */
    int idx;

    public boolean found() {
        return idx >= 0;
    }
    public BLeafNode get_leaf() {
        BTreeNode node = path.get(path.size() - 1);
        BLeafNode leaf = new BLeafNode(node.get_page_id(), node.get_owner());
        return leaf;
    }
    public Payload get_value() {
        if (!found()) throw new DBRuntimeError("not found");
        int idx = idxs.get(idxs.size() - 1);
        return get_leaf().get_value(idx);
    }

    public void set_value(Payload value) {
        if (!found()) throw new DBRuntimeError("not found");
        int idx = idxs.get(idxs.size() - 1);
        get_leaf().set_value(idx, value);
    }
}
