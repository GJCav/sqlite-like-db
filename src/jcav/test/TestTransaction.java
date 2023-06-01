package jcav.test;

import jcav.filelayer.BTreeTable;
import jcav.filelayer.DBFile;
import jcav.filelayer.Transaction;
import jcav.filelayer.btree.BTreeNode;
import jcav.filelayer.btree.ObjType;
import jcav.filelayer.btree.Payload;
import jcav.filelayer.btree.SearchResult;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TestTransaction {
    public static void main(String[] argv){
        //////////////////////////////////////////////////
        // test base, add some initial data
        /////////////////////////////////////////////////
        try{
            if (Files.exists(Paths.get("test.db"))) {
                Files.delete(Paths.get("test.db"));
            }
        } catch (Exception e) {e.printStackTrace();}

        try(DBFile db = DBFile.create("test.db")) {
            db.use_LRUCache();

            List<Integer> key_types = Arrays.asList(ObjType.INT, ObjType.STRING(64));
            List<Integer> val_types = Arrays.asList(ObjType.STRING(32));
            BTreeTable table = BTreeTable.create(
                    db,
                    "student",
                    key_types,
                    val_types
            );

            for(int i = 0;i < 10;i++) {
                table.insert(
                        Payload.creator().val(i).val(64, "key " + i).create(),
                        Payload.creator().val(32, "name " + i).create()
                );
            }

            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        ///////////////////////////////////////////////////////////////////////
        // transaction without commit and rollback, default action is rollback
        ///////////////////////////////////////////////////////////////////////
        System.out.println("-------------------------------------------------");
        try(DBFile db = new DBFile("test.db")) {
            db.use_LRUCache();
            System.out.println("begin transaction");
            BTreeTable table = db.get_schema().get_table("student");
            try(Transaction tx = db.transaction()) {
                for(int i = 10;i < 15;i++) {
                    System.out.println("inserting " + i);
                    table.insert(
                            Payload.creator().val(i).val(64, "key " + i).create(),
                            Payload.creator().val(32, "name " + i + " new").create()
                    );
                }

                for(int i = 0;i < 5;i++) {
                    SearchResult sr = table.search(
                            Payload.creator().val(i).val(64, "key " + i).create()
                    );
                    sr.set_value(
                            Payload.creator().val(32, "name " + i + " updated").create()
                    );
                }

                System.out.println("During transaction:");
                table._print_leaf_nodes();
            }
            System.out.println("default action is rollback, so nothing changed");
            System.out.println("After transaction:");
            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //////////////////////////////////////////////////////////////////////
        // transaction with commit-on-close.
        // this is the STANDARD way to use transaction, you should always use this.
        // You can call tx.commit() to commit the transaction, but it is DISCOURAGED.
        // after commit, the transaction is closed and tx cannot be used again.
        //////////////////////////////////////////////////////////////////////
        System.out.println("-------------------------------------------------");
        System.out.println("this time we will commit the transaction, you will see the changes");
        try(DBFile db = new DBFile("test.db")) {
            db.use_LRUCache();
            System.out.println("begin transaction");
            BTreeTable table = db.get_schema().get_table("student");
            try(Transaction tx = db.transaction()) {
                tx.commit_on_close();

                for(int i = 10;i < 15;i++) {
                    table.insert(
                            Payload.creator().val(i).val(64, "key " + i).create(),
                            Payload.creator().val(32, "name " + i + " new").create()
                    );
                }

                for(int i = 0;i < 5;i++) {
                    SearchResult sr = table.search(
                            Payload.creator().val(i).val(64, "key " + i).create()
                    );
                    sr.set_value(
                            Payload.creator().val(32, "name " + i + " updated").create()
                    );
                }
                System.out.println("end transaction");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        // reopen the db and check the changes
        try(DBFile db = new DBFile("test.db")) {
            BTreeTable table = db.get_schema().get_table("student");
            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        //////////////////////////////////////////////////////////////////////
        // test recovery from rollback state
        // if the transaction is not committed, and the program is crashed,
        // the transaction is in rollback state and will be rolled back when
        // the program is restarted.
        //////////////////////////////////////////////////////////////////////
        System.out.println("-------------------------------------------------");
        try(DBFile db = new DBFile("test.db")) {
            db.use_LRUCache();
            Transaction tx = db.transaction();

            BTreeTable table = db.get_schema().get_table("student");
            for(int i = 5;i < 10;i++){
                SearchResult sr = table.search(
                        Payload.creator().val(i).val(64, "key " + i).create()
                );
                sr.set_value(
                        Payload.creator().val(32, "name " + i + " rollback recovery").create()
                );
            }

            // thanks to reflection we can use some magic to test the recovery
            // we will neither commit nor rollback the transaction, but close it directly
            // this will release file handle to the WAL file but the WAL file is not deleted
            Field dec = Transaction.class.getDeclaredField("wal_db");
            dec.setAccessible(true);
            DBFile wal_db = (DBFile)dec.get(tx);
            wal_db.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("without tx.close");
        System.out.println("check the existence of the WAL file in the disk");
        pause();

        System.out.println("reopen the db, if everything works fine, ");
        System.out.println("the db will recover from the WAL file by rollback");
        System.out.println("you won't see the changes made by the transaction");
        System.out.println("and the WAL file will be deleted");
        try(DBFile db = new DBFile("test.db")) {
            BTreeTable table = db.get_schema().get_table("student");
            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        pause();

        //////////////////////////////////////////////////////////////////////
        // test recovery from committing state
        // if the transaction is committing, and the program is crashed.
        // the transaction is in committing state and will be committed when
        // the program is restarted.
        //////////////////////////////////////////////////////////////////////
        System.out.println("-------------------------------------------------");
        System.out.println("-------------------------------------------------");
        try(DBFile db = new DBFile("test.db")) {
            db.use_LRUCache();
            Transaction tx = db.transaction();

            BTreeTable table = db.get_schema().get_table("student");
            for(int i = 5;i < 10;i++){
                SearchResult sr = table.search(
                        Payload.creator().val(i).val(64, "key " + i).create()
                );
                sr.set_value(
                        Payload.creator().val(32, "name " + i + " commit recovery").create()
                );
            }

            // thanks to reflection we can use some magic to test the recovery.
            // we will sync the WAL file to the disk, mark it as COMMITTING without writing back.
            // this will release file handle to the WAL file but the WAL file is not deleted
            Field dec = Transaction.class.getDeclaredField("wal_db");
            dec.setAccessible(true);
            DBFile wal_db = (DBFile)dec.get(tx);
            wal_db.get_headers().set("w_state", Transaction.W_COMMITTING);
            wal_db.sync();
            wal_db.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.out.println("without complete commit");
        System.out.println("check the existence of the WAL file in the disk");
        pause();

        System.out.println("reopen the db, if everything works fine, ");
        System.out.println("the db will recover from the WAL file by committing");
        System.out.println("you will see the changes made by the transaction");
        System.out.println("and the WAL file will be deleted");
        try(DBFile db = new DBFile("test.db")) {
            db.use_LRUCache();
            BTreeTable table = db.get_schema().get_table("student");
            table._print_leaf_nodes();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static void pause() {
        // pause the standard input
        try{
            System.out.println("press something and enter to continue");
            System.in.read();
        } catch (Exception e) {}
    }
}
