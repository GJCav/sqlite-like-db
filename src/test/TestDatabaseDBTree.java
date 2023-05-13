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
            db.set_cache(new LRUCache(db, 3));

            int page_id = db.alloc_page();
            List<Integer> key_types = Arrays.asList(ObjType.INT, ObjType.INT);
            List<Integer> val_types = Arrays.asList(ObjType.STRING(32));
            BPlusTree tree = BPlusTree.create(
                    page_id,
                    db,
                    key_types,
                    val_types
            );

            int count = 10;
            for(int i = 0;i <= count;i++) {
                Payload key = Payload.create(key_types, Arrays.asList(i, i));
                Payload value = Payload.create(val_types, Arrays.asList("hello worl" + i));

                System.out.println("inserting " + key + " -> " + value);
                tree.insert(key, value);
                tree._print_tree();
                tree._check_total();
            }

            tree._print_leaf_nodes();

            System.out.println("---------------- delete -------------------");
            tree._print_tree();
            for(int i = count;i >=0 ;i--){
                Payload key = Payload.create(key_types, Arrays.asList(i, i));
                SearchResult sr = tree.search(key);
                System.out.println("deleting " + key);
                tree.delete(sr);

                tree._print_tree();
                tree._check_total();
            }

            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
