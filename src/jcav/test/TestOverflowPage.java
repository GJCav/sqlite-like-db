package jcav.test;

import jcav.filelayer.Bytes;
import jcav.filelayer.DBFile;
import jcav.filelayer.LRUCache;
import jcav.filelayer.OverflowPage;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestOverflowPage {
    public static void main(String[] args) {
        try{
            if(Files.exists(Paths.get("test.db"))){
                Files.delete(Paths.get("test.db"));
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        try {
            DBFile db = DBFile.create("test.db");
            db.set_cache(new LRUCache(db, 10));

            // 1st page is overflow page
            int page_id = db.alloc_page();
            OverflowPage page = OverflowPage.create(page_id, db);

            // make some space
            db.alloc_page(); // 2ed page is null page
            int rel = db.alloc_page(); // 3rd page is overflow page
            db.alloc_page(); // null
            db.alloc_page(); // null
            db.alloc_page(); // null
            db.release_page(rel);

            OutputStream out = page.get_output_stream(32);
            for(int i = 0; i < 48; i++){
                out.write(Bytes.from_string("Hello World!"));
            }

            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(DBFile db = new DBFile("test.db")){
            DumpDB d = new DumpDB(db);
            d.print_overflow_chain(1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
