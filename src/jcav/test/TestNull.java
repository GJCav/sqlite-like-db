package jcav.test;


import jcav.filelayer.BTreeTable;
import jcav.filelayer.DBFile;
import jcav.filelayer.LRUCache;
import jcav.filelayer.btree.ObjType;
import jcav.filelayer.btree.Payload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TestNull {
    public static void main(String[] args) {
        try {
            if (Files.exists(Paths.get("test.db"))) {

                Files.delete(Paths.get("test.db"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        try (DBFile db = DBFile.create("test.db")) {
            db.set_cache(new LRUCache(db, 20));

            List<Integer> key_types = Arrays.asList(ObjType.INT, ObjType.INT);
            List<Integer> val_types = Arrays.asList(ObjType.INT, ObjType.STRING(32));
            BTreeTable table = BTreeTable.create(
                    db,
                    "test_table",
                    key_types,
                    val_types
            );

            table.insert(
                    Payload.create(key_types, Arrays.asList(1, null)),
                    Payload.create(val_types, Arrays.asList(1, null))
            );

            table.insert(
                    Payload.create(key_types, Arrays.asList(null, 2)),
                    Payload.create(val_types, Arrays.asList(2, null))
            );

            table.insert(
                    Payload.create(key_types, Arrays.asList(null, null)),
                    Payload.create(val_types, Arrays.asList(null, "three"))
            );

        } catch (Exception e) {
            e.printStackTrace();
        }

        try (DBFile db = new DBFile("test.db")) {
            db.set_cache(new LRUCache(db, 20));
            BTreeTable table = db.get_schema().get_table("test_table");
            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
