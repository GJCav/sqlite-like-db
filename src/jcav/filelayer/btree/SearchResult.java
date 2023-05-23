package jcav.filelayer.btree;

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
}
