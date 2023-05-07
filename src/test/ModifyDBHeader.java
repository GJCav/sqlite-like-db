package test;

import db.*;

public class ModifyDBHeader {
    public static void main(String[] argv) {
        try {
            DBFile db = new DBFile("test.db");
            db.set_cache(new LRUCache(db, 3));

            Headers headers = db.get_headers();
            headers.set("file_id", "ILOVEPKU");
            headers.set("ver", (byte)2);
            headers.set("page_size", (byte)14);
            headers.set("page_count", 1);
            headers.set("freelist_head", -1);
            headers.set("freelist_count", 100);
            headers.set("cache_count", 10000);

            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
