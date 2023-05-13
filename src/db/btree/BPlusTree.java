package db.btree;

import db.DBFile;
import db.Page;
import db.PageType;
import db.exception.DBRuntimeError;

import java.util.Collections;
import java.util.List;

public class BPlusTree {
    private BTreeNode root;
    private DBFile db;

    public BPlusTree(int root_page, DBFile db) {
        this.db = db;
        this.root = new BTreeNode(root_page, db);
    }

    public static BPlusTree create(int page_id, DBFile db, List<Integer> key_types, List<Integer> val_types) {
        if (page_id == 0) throw new IllegalArgumentException("page_id must not be 0");
        if (key_types == null) throw new NullPointerException("key_types must not be null");
        if (val_types == null) throw new NullPointerException("val_types must not be null");

        int[] key_types_arr = new int[key_types.size()];
        for (int i = 0; i < key_types.size(); i++) {
            key_types_arr[i] = key_types.get(i);
        }
        int[] val_types_arr = new int[val_types.size()];
        for (int i = 0; i < val_types.size(); i++) {
            val_types_arr[i] = val_types.get(i);
        }
        BLeafNode.create(
                page_id,
                db,
                key_types_arr,
                val_types_arr
        );
        return new BPlusTree(page_id, db);
    }

    private void check_key_types(Payload key) {
        List<Integer> key_types = root.get_key_type_list();
        if (!Payload.is_compatible(key_types, key.get_types())) {
            throw new IllegalArgumentException("key type mismatch, " +
                    "expect " + ObjType.to_string(key_types) +
                    ", got " + ObjType.to_string(key.get_types()));
        }
    }

    private void check_val_types(Payload value) {
        List<Integer> val_types = root.get_value_type_list();
        if (!Payload.is_compatible(val_types, value.get_types())) {
            throw new IllegalArgumentException("value type mismatch, " +
                    "expect " + ObjType.to_string(val_types) +
                    ", got " + ObjType.to_string(value.get_types()));
        }
    }

    public SearchResult search(Payload key) {
        check_key_types(key);

        SearchResult r = new SearchResult();
        BTreeNode cur = root;

        while(cur != null) {
            r.path.add(cur);
            if (cur.get_page_type() == PageType.BTREE_INTERIOR) {
                BInteriorNode interior = new BInteriorNode(cur.get_page_id(), db);
                List<Payload> keys = interior.get_keys();
                int idx = Collections.binarySearch(keys, key);
                if (idx < 0) {
                    idx = -(idx+1);
                }
                int child_page = interior.get_child(idx);
                cur = new BTreeNode(child_page, db);
                r.idxs.add(idx);
            } else if (cur.get_page_type() == PageType.BTREE_LEAF) {
                BLeafNode leaf = new BLeafNode(cur.get_page_id(), db);
                List<Payload> keys = leaf.get_keys();
                int idx = Collections.binarySearch(keys, key);
                r.idx = idx;
                r.idxs.add(idx);
                cur = null;
            } else {
                throw new DBRuntimeError("Invalid page type, " +
                        "expect BTREE_INTERIOR or BTREE_LEAF, " +
                        "got " + cur.get_page_type());
            }
        }
        return r;
    }

    public void insert(Payload key, Payload value) {
        check_key_types(key);
        check_val_types(value);

        SearchResult r = search(key);
        BTreeNode node = r.path.get(r.path.size() - 1);
        if (node.get_page_type() != PageType.BTREE_LEAF) {
            throw new DBRuntimeError("find error");
        }

        BLeafNode leaf = new BLeafNode(node.get_page_id(), db);
        if (r.found()) {
            throw new IllegalArgumentException("key already exists");
        }

        if (leaf.get_slot_count() + 1 > leaf.get_slot_capacity()) {
            SplitResult sr = leaf.split();
            if (sr.root_page_id != 0) {
                root = new BTreeNode(sr.root_page_id, db);
            }
            int cmp = key.compareTo(sr.key);
            if (cmp > 0) {
                leaf = new BLeafNode(sr.right.get_page_id(), db);
            } else if (cmp < 0) {
                leaf = new BLeafNode(sr.left.get_page_id(), db);
            } else {
                throw new DBRuntimeError("split error");
            }
        }

        leaf.insert(key, value);
    }

    public void delete(SearchResult sr) {
        if (!sr.found()) {
            throw new IllegalArgumentException("key not found");
        }

        BLeafNode leaf = new BLeafNode(
                sr.path.get(sr.path.size() - 1).get_page_id(),
                db
        );
        DeleteResult dr = leaf.delete(sr);
        if (dr.root_page_id != 0) {
            root = new BTreeNode(dr.root_page_id, db);
        }
    }

    public BLeafNode leftmost_leaf() {
        BTreeNode cur = root;
        while (cur.get_page_type() == PageType.BTREE_INTERIOR) {
            BInteriorNode interior = new BInteriorNode(cur.get_page_id(), db);
            cur = new BTreeNode(interior.get_child(0), db);
        }
        return new BLeafNode(cur.get_page_id(), db);
    }
}
