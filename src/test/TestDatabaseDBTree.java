package test;

import db.DBFile;
import db.LRUCache;
import db.btree.*;
import db.btree.BPlusTree;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TestDatabaseDBTree {
    public static void main(String[] args) {
        try {
            if (Files.exists(Paths.get("test.db"))) {
                Files.delete(Paths.get("test.db"));
            }

            DBFile db = DBFile.create("test.db");
            db.set_cache(new LRUCache(db, 100));

            int page_id = db.alloc_page();
            List<Integer> key_types = Arrays.asList(ObjType.INT, ObjType.INT);
            List<Integer> val_types = Arrays.asList(ObjType.STRING(32));
            BPlusTree tree = BPlusTree.create(
                    page_id,
                    db,
                    key_types,
                    val_types
            );

            for(int i = 0;i < 20;i++) {
                Payload key = Payload.create(key_types, Arrays.asList(i, i));
                Payload value = Payload.create(val_types, Arrays.asList("hello world " + i));

                System.out.println("inserting " + key + " -> " + value);
                tree.insert(key, value);
            }

            BLeafNode lf = tree.leftmost_leaf();
            while(lf != null) {
                System.out.println("leaf " + lf.get_page_id());
                List<Integer> slots = lf.get_slots();
                for(int idx : slots) {
                    System.out.println("  " + lf.get_key(idx) + " -> " + lf.get_value(idx));
                }

                int right_id = lf.get_right_sibling();
                if (right_id != 0) {
                    lf = new BLeafNode(right_id, db);
                } else {
                    lf = null;
                }
            }

            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
