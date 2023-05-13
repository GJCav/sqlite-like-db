package db.btree;

import java.util.ArrayList;
import java.util.List;

public class SearchResult {
    public List<BTreeNode> path = new ArrayList<>();
    public List<Integer> idxs = new ArrayList<>();

    int idx;

    public boolean found() {
        return idx >= 0;
    }
}
