package test;

import db.DBFile;
import db.LRUCache;
import db.btree.*;
import db.btree.BPlusTree;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDBTreeRW {
    public static void main(String[] args) {
        int count = 1000;
        int root_page = 0;
        int max_cache = 300;

        long st = System.currentTimeMillis();

        try {
            if (Files.exists(Paths.get("test.db"))) {
                Files.delete(Paths.get("test.db"));
            }

            DBFile db = DBFile.create("test.db");
            db.set_cache(new LRUCache(db, max_cache));

            int page_id = db.alloc_page();
            List<Integer> key_types = Arrays.asList(ObjType.INT, ObjType.INT);
            List<Integer> val_types = Arrays.asList(ObjType.STRING(32));
            db.btree.BPlusTree tree = BPlusTree.create(
                    page_id,
                    db,
                    key_types,
                    val_types
            );

            for(int i = 0;i <= count;i++) {
//                System.out.println("insert " + i);
                Payload key = Payload.create(key_types, Arrays.asList(i, i));
                Payload value = Payload.create(val_types, Arrays.asList("hello worl" + i));
                tree.insert(key, value);
            }

            root_page = tree.root_page();
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("finish write, " + (System.currentTimeMillis() - st) + "ms");

        st = System.currentTimeMillis();
        int[] del_mod = new int[]{2, 3, 5, 7, 11, 13, 17, 19, 23};
        try {
            DBFile db = new DBFile("test.db");
            db.set_cache(new LRUCache(db, max_cache));

            BPlusTree tree = new BPlusTree(root_page, db);
            BLeafNode lf = tree.leftmost_leaf();

            int i = 0;
            while(lf != null) {
                int c = lf.get_slot_count();
                for (int j = 0;j < c;j++) {
                    Payload key = lf.get_key(j);
                    Payload value = lf.get_value(j);

                    assert key.get_obj(0).as_int() == i;
                    assert value.get_obj(0).as_string().equals("hello worl" + i);
                    i++;
                }
                int next = lf.get_right_sibling();
                if (next != 0) {
                    lf = new BLeafNode(next, db);
                } else {
                    lf = null;
                }
            }

            System.out.println("pass read test, " + (System.currentTimeMillis() - st) + "ms");

            System.out.println("start delete test");
            st = System.currentTimeMillis();
            for (int k = 0;k <= count;k++) {
                boolean del = false;
                for(int mod : del_mod) {
                    if (k % mod == 0) {
                        del = true;
                        break;
                    }
                }

                if (del){
                    Payload key = Payload.create(Arrays.asList(ObjType.INT, ObjType.INT), Arrays.asList(k, k));
                    SearchResult rs = tree.search(key);
                    if (rs.found()) {
                        tree.delete(rs);
                    }
                }
            }
            System.out.println("finish delete test, " + (System.currentTimeMillis() - st) + "ms");
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("check delete result");
        List<Integer> kept_vals = new ArrayList<>();
        for(int i = 0; i <= count;i++) {
            boolean del = false;
            for(int mod : del_mod) {
                if (i % mod == 0) {
                    del = true;
                    break;
                }
            }

            if (!del) {
                kept_vals.add(i);
            }
        }
        System.out.println("kept vals: " + kept_vals.size());
        st = System.currentTimeMillis();
        try {
            DBFile db = new DBFile("test.db");
            db.set_cache(new LRUCache(db, max_cache));

            BPlusTree tree = new BPlusTree(root_page, db);
            BLeafNode lf = tree.leftmost_leaf();

            int i = 0;
            while(lf != null) {
                int c = lf.get_slot_count();
                for (int j = 0;j < c;j++) {
                    Payload key = lf.get_key(j);
                    Payload value = lf.get_value(j);

                    assert key.get_obj(0).as_int() == kept_vals.get(i);
                    assert value.get_obj(0).as_string().equals("hello worl" + kept_vals.get(i));
                    i++;
                }
                int next = lf.get_right_sibling();
                if (next != 0) {
                    lf = new BLeafNode(next, db);
                } else {
                    lf = null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        System.out.println("pass delete test, " + (System.currentTimeMillis() - st) + "ms");
    }
}
