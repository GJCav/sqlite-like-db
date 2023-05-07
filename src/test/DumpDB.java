package test;

import db.*;

import java.io.IOException;

public class DumpDB {
    public static void main(String[] argv) {
        try {
            DBFile db = new DBFile("test.db");
            db.set_cache(new SimpleCache(db, 3));
            DumpDB d = new DumpDB(db);
            d.print_db_header();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private DBFile db;
    private int level;

    public DumpDB(DBFile db) {
        this.db = db;
    }

    public void print_db_header() {
        Headers headers = db.get_headers();
        println("DB Header:");
        for (FieldDef def : headers.field_defs) {
            StringBuilder info = new StringBuilder();
            info.append("- ").append(def.name).append(": ");
            try {
                String val = headers.get(def.name).to_object().toString();
                if (def.name == "page_size") {
                    int m = Integer.parseInt(val);
                    val = String.format("%d (%d bytes)", m, 1<<m);
                }
                info.append(val);
            } catch (IOException e) {
                info.append("ERROR, " + e.getMessage());
            }
            println(info.toString());
        }
    }

    public void print_free_pages() {
        println("Free Pages (with first 32 bytes as str):");
        int free_page = 0;
        try {
            free_page = db.get_headers().get("freelist_head").to_int();
        } catch (IOException e) {
            println("ERROR, " + e.getMessage());
            e.printStackTrace();
        }

        while(free_page != 0) {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append("- page_id=").append(free_page).append(": ");

            Page page = new FreePage(free_page, db);
            try {
                Headers headers = page.get_headers();
                int next_free = headers.get("next_free").to_int();
                int type = headers.get("type").to_byte();
                sbuf.append(String.format("type=%d, next_free=%d, ", type, next_free));
                int offset = headers.get_total_length();
                byte[] buf = db.read(free_page, offset, offset+32);
                sbuf.append("partial_data=").append(Bytes.to_string(buf));
                free_page = next_free;
            } catch (IOException e) {
                sbuf.append("ERROR, " + e.getMessage());
                free_page = 0;
            }
            println(sbuf.toString());
        }
    }

    private void println(String s) {
        for (int i = 0; i < this.level; i++) {
            System.out.print("  ");
        }
        System.out.println(s);
    }

    public void print_all_pages() {
        int page_count = -1;
        try {
            page_count = db.get_headers().get("page_count").to_int();
        } catch (IOException e) {
            e.printStackTrace();
            page_count = -1;
        }

        if (page_count == -1) {
            println("ERROR, cannot get page_count");
            return;
        }

        println("All Pages (with first 32 bytes as str):");
        println("- Page count: " + page_count);
        println("- Headers: ");
        level++;
        print_db_header();
        level--;
        for (int i = 1;i < page_count;i++) {
            println("- Page " + i + ": ");
            level++;
            StringBuilder sbuf = new StringBuilder();
            sbuf.append("- type: ");

            Page page = new Page(i, db);
            int type = -1;
            try {
                type = page.get_headers().get("type").to_byte();
            } catch (IOException e) {
                sbuf.append("ERROR, " + e.getMessage());
            }


            if (type == Page.TYPE_NULL) {
                sbuf.append(type).append(" , null page");
                println(sbuf.toString());

                sbuf = new StringBuilder();
                sbuf.append("- first 32 bytes: ");
                try {
                    int pos = page.get_headers().get_total_length();
                    byte[] buf = db.read(i, pos, 32);
                    sbuf.append(Bytes.to_string(buf));
                } catch (IOException e) {
                    sbuf.append("ERROR, " + e.getMessage());
                }
                println(sbuf.toString());
            } else if (type == Page.TYPE_FREE) {
                sbuf.append(type).append(" , free page");
                println(sbuf.toString());

                sbuf = new StringBuilder();
                sbuf.append("- first 32 bytes: ");
                page = new FreePage(i, db);
                try {
                    int pos = page.get_headers().get_total_length();
                    byte[] buf = db.read(i, pos, 32);
                    sbuf.append(Bytes.to_string(buf));
                } catch (IOException e) {
                    sbuf.append("ERROR, " + e.getMessage());
                }
                println(sbuf.toString());
            } else {
                sbuf.append(type).append(" , unknown");
                println(sbuf.toString());
            }
            level--;
        }
    }
}
