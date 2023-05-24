package jcav.filelayer.btree;

import jcav.filelayer.DBFile;
import jcav.filelayer.Page;
import jcav.filelayer.PageType;
import jcav.filelayer.exception.DBRuntimeError;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

public class BPlusTree {
    private BTreeNode root;
    private DBFile db;

    ////////////////////////////////////////////////////////////////////
    // life cycle
    ////////////////////////////////////////////////////////////////////

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

    protected void release_self() {
        recursive_drop(root);
    }

    private void recursive_drop(BTreeNode h) {
        DBFile db = this.get_db();

        if (h.get_page_type() == PageType.BTREE_INTERIOR) {
            BInteriorNode interior = new BInteriorNode(h.get_page_id(), this.get_db());
            int[] children = interior.get_children();
            for(int child : children) {
                BTreeNode child_node = new BTreeNode(child, get_db());
                recursive_drop(child_node);
            }

            db.release_page(h.get_page_id());
        } else if (h.get_page_type() == PageType.BTREE_LEAF) {
            BLeafNode leaf = new BLeafNode(h.get_page_id(), this.get_db());
            leaf.release_self();
        } else {
            throw new DBRuntimeError("invalid page type " + h.get_page_type() + ", fuck");
        }
    }

    //////////////////////////////////////////////////////////////////////
    // others
    //////////////////////////////////////////////////////////////////////

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

    public Payload get_value(SearchResult sr){
        if (!sr.found()) throw new IllegalArgumentException("key not found");
        BLeafNode leaf = new BLeafNode(
                sr.path.get(sr.path.size() - 1).get_page_id(),
                db
        );
        Payload payload = leaf.get_value(sr.idx);
        return payload;
    }

    public void set_value(SearchResult sr, Payload val) {
        if (!sr.found()) throw new IllegalArgumentException("key not found");
        BLeafNode leaf = new BLeafNode(
                sr.path.get(sr.path.size()-1).get_page_id(),
                db
        );
        leaf.set_value(sr.idx, val);
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

    public int root_page() {
        return root.get_page_id();
    }

    public DBFile get_db() {
        return db;
    }

    public List<Integer> get_key_types() {
        return root.get_key_type_list();
    }

    public List<Integer> get_value_types() {
        return root.get_value_type_list();
    }

    public void foreach_leaf(Consumer<BLeafNode> consumer) {
        BLeafNode leaf = leftmost_leaf();
        while (leaf != null) {
            consumer.accept(leaf);
            int next = leaf.get_right_sibling();
            if (next != 0) {
                leaf = new BLeafNode(next, db);
            } else {
                leaf = null;
            }
        }
    }

    ////////////////////////////////////////////////////////////
    // for debug
    ////////////////////////////////////////////////////////////

    public void _print_tree() {
        Queue<Integer> q = new LinkedList<>();
        Queue<Integer> d = new LinkedList<>();
        q.add(root.get_page_id());
        d.add(0);

        int ld = 0;
        while(q.size() > 0) {
            int page_id = q.remove();
            int cd = d.remove();
            if (cd != ld) {
                System.out.println();
                ld = cd;
            }

            BTreeNode node = new BTreeNode(page_id, db);
            StringBuilder sbuf = new StringBuilder("(");
            if (node.get_page_type() == PageType.BTREE_INTERIOR) {
                BInteriorNode interior = new BInteriorNode(page_id, db);
                for(Payload p : interior.get_keys()) {
                    sbuf.append(p.get_obj(0));
                    sbuf.append(",");
                }
                int cnt = interior.get_slot_count();
                for(int i = 0;i <= cnt;i++) {
                    q.add(interior.get_child(i));
                    d.add(cd+1);
                }
            } else if (node.get_page_type() == PageType.BTREE_LEAF){
                BLeafNode leaf = new BLeafNode(page_id, db);
                for(Payload p : leaf.get_keys()) {
                    sbuf.append(p.get_obj(0));
                    sbuf.append(",");
                }
            } else {
                throw new DBRuntimeError("Invalid page type, " +
                        "expect BTREE_INTERIOR or BTREE_LEAF, " +
                        "got " + node.get_page_type());
            }
            sbuf.append(")");

            int fth = node.get_father();
            if(fth != 0) {
                BInteriorNode interior = new BInteriorNode(fth, db);
                Payload key = interior.get_keys().get(0);
                sbuf.append("^").append(key.get_obj(0));
            }
            sbuf.append("    ");
            System.out.print(sbuf.toString());
        }
        System.out.println();
    }

    public void _print_leaf_nodes() {
        foreach_leaf((lf) -> {
            System.out.println("leaf " + lf.get_page_id());
            for(int i = 0;i < lf.get_slot_count();i++) {
                System.out.println("- " + lf.get_key(i) + " -> " + lf.get_value(i));
            }
        });
    }

    public int _check_total(BTreeNode h) {
        if (h.get_page_type() == PageType.BTREE_LEAF) {
            BLeafNode leaf = new BLeafNode(h.get_page_id(), db);
            int total = leaf.get_total();

            if (total != leaf.get_slot_count()) {
                throw new RuntimeException("total error, expect " + leaf.get_slot_count() + ", get " + total);
            }
            return leaf.get_slot_count();
        }

        int total = 0;
        int child_cnt = h.get_slot_count()+1;
        BInteriorNode node = new BInteriorNode(h.get_page_id(), db);
        for(int i = 0;i < child_cnt;i++){
            BTreeNode child = new BTreeNode(node.get_child(i), db);
            total += _check_total(child);
        }
        if (total != h.get_total()) {
            throw new RuntimeException("total error, expect " + total
                    + ", get " + h.get_total() + ", at (" + _print_keys(node) + ")");
        }
        return total;
    }

    public String _print_keys(BInteriorNode node) {
        StringBuilder sbuf = new StringBuilder();
        for(Payload p : node.get_keys()) {
            sbuf.append(p.get_obj(0));
            sbuf.append(",");
        }
        return sbuf.toString();
    }

    public void _check_total() {
        _check_total(root);
    }

    public void _check_child(int page_id) {
        Page page = new Page(page_id, db);
        int type = page.get_page_type();
        if (type == PageType.BTREE_LEAF) return;
        else if (type == PageType.BTREE_INTERIOR) {
            BInteriorNode node = new BInteriorNode(page_id, db);
            int[] children = node.get_children();
            for(int child_id : children) {
                if (child_id == 0) {
                    throw new RuntimeException("unexpected child error, at (" + _print_keys(node) + ")");
                }
                _check_child(child_id);
            }
        } else {
            throw new RuntimeException("Invalid page type, " +
                    "expect BTREE_INTERIOR or BTREE_LEAF, " +
                    "got " + type);
        }
    }
}
