package test;

import db.*;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class CreateReleasePage {
    public static void main(String[] argv) {
        List<Integer> pages = new ArrayList<>();

        try {
            if (Files.exists(new File("test.db").toPath())) {
                Files.delete(new File("test.db").toPath());
            }
            DBFile db = DBFile.create("test.db");
            db.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

        // write to null page
        try (DBFile db = new DBFile("test.db");) {

            db.set_cache(new SimpleCache(db, 3));

            for (int i = 0;i < 10;i++) {
                int page_id = db.alloc_page();
                pages.add(page_id);

                db.write(
                        page_id,
                        Headers.get_total_length(Page.HEADER_DEFS),
                        Bytes.from_string("some data " + i)
                );
            }

            DumpDB d = new DumpDB(db);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // read from null page and release them
        try (DBFile db = new DBFile("test.db");) {
            db.set_cache(new SimpleCache(db, 3));
            DumpDB d = new DumpDB(db);

            System.out.println("Null Page data: ");
            for (int page_id : pages) {
                byte[] data = db.read(page_id, Headers.get_total_length(Page.HEADER_DEFS), 32);
                String s = Bytes.to_string(data);
                System.out.println("- page_id=" + page_id + ": " + s);

                db.release_page(page_id);
            }

            d.print_free_pages();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // re check the header
        System.out.println("----------------------------------");
        try (DBFile db = new DBFile("test.db")) {
            DumpDB d = new DumpDB(db);
            d.print_db_header();
            d.print_free_pages();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // alloc some page and write some data
        System.out.println("--------- re allocate ----------");
        try (DBFile db = new DBFile("test.db")) {
            for (int i = 0;i < 3;i++) {
                int page_id = db.alloc_page();

                db.write(
                        page_id,
                        Headers.get_total_length(Page.HEADER_DEFS),
                        Bytes.from_string("after reallocate " + i)
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (DBFile db = new DBFile("test.db")) {
            db.set_cache(new SimpleCache(db, 1));

            DumpDB d = new DumpDB(db);
            d.print_all_pages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
