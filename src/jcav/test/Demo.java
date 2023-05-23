package jcav.test;

import jcav.filelayer.BTreeTable;
import jcav.filelayer.DBFile;
import jcav.filelayer.LRUCache;
import jcav.filelayer.btree.*;
import jcav.filelayer.exception.DBRuntimeError;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class Demo {
    public static void main(String[] args) {
        // Demo to show the main API of the DB.

        ///////////////////////////////
        // DB creation
        ///////////////////////////////
        try {
            if (Files.exists(Paths.get("test.db"))) {
                Files.delete(Paths.get("test.db"));
            }

            // in my API design, if you want to create something
            // always use static method from the class.
            // this code creates a new database file at the path.
            DBFile db = DBFile.create("test.db");

            // remember to close the db when you are done.
            // this will sync the cache and close the file.
            // if you don't close the db, some data may still in cache
            // and won't be written to the file.
            db.close();
        } catch (DBRuntimeError e) {
            // when DBRuntimeError occurs, something terrible happened.
            // either JCav have written a bug, or you have done something
            // terribly wrong. See the error message for more information.

            System.out.println(e.getMessage());
            System.out.println("Cause: " + e.getCause());
            e.printStackTrace();
        } catch (Exception e) {
            // other exceptions are usually caused by IO errors.
            e.printStackTrace();
        }


        ///////////////////////////////
        // open an existing DB
        //   - create a new table
        //   - insert some data
        ///////////////////////////////
        try {
            // in my API design, if you want to open something
            // always initialize an object from the class.
            DBFile db = new DBFile("test.db");

            // switch to LRU cache policy instead of NO cache.
            db.set_cache(new LRUCache(
                    db,
                    db.get_headers().get("cache_count").to_int())
            );

            // create a new table
            List<Integer> key_types = Arrays.asList(ObjType.INT);
            List<Integer> value_types = Arrays.asList(ObjType.INT, ObjType.STRING(32));
            BTreeTable table = BTreeTable.create(db, "student", key_types, value_types);

            for (int i = 0;i < 10;i++) {
                // insert some data
                table.insert(
                        Payload.create(key_types, Arrays.asList(i)),
                        Payload.create(value_types, Arrays.asList(i, "STU_" + i))
                );
            }

            db.close();
        } catch (DBRuntimeError e) {
            System.out.println(e.getMessage());
            System.out.println("Cause: " + e.getCause());
            e.getCause().printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        ///////////////////////////////////
        // open an existing DB
        //  - open an existing table
        //  - search for some data
        //  - delete some data
        //  - iterate through the table
        //  - drop the table
        ///////////////////////////////////
        try {
            DBFile db = new DBFile("test.db");
            db.set_cache(new LRUCache(
                    db,
                    db.get_headers().get("cache_count").to_int())
            );

            // open an existing table
            BTreeTable table = db.get_schema().get_table("student");

            // search for some data
            SearchResult sr = table.search(
                    Payload.create(Arrays.asList(ObjType.INT), Arrays.asList(5))
            );
            if (sr.found()) {
                int id = table.get_value(sr).get_obj(0).as_int();
                String name = table.get_value(sr).get_obj(1).toString();
                System.out.println("id: " + id + ", name: " + name);
            } else {
                throw new RuntimeException("data not found");
            }

            // delete some data
            table.delete(sr);

            // iterate through the table
            BLeafNode leaf = table.leftmost_leaf();
            while (leaf != null) {
                for (int i = 0;i < leaf.get_slot_count();i++) {
                    Payload key = leaf.get_key(i);
                    Payload value = leaf.get_value(i);
                    System.out.println("key: " + key + ", value: " + value);
                }

                int next = leaf.get_right_sibling();
                if (next != 0) {
                    leaf = new BLeafNode(next, leaf.get_owner());
                } else {
                    leaf = null;
                }
            }

            // drop the table
            table.drop_self();

            db.close();
        } catch (DBRuntimeError e) {
            System.out.println(e.getMessage());
            System.out.println("Cause: " + e.getCause());
            e.getCause().printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
