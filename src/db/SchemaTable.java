package db;

import db.btree.BPlusTree;
import db.btree.ObjType;
import db.btree.Payload;
import db.btree.SearchResult;
import db.exception.DBRuntimeError;

import java.util.Arrays;

public class SchemaTable extends BPlusTree {

    protected SchemaTable(int root_page, DBFile db) {
        super(root_page, db);
    }

    @Override
    public void insert(Payload key, Payload value) {
        int old_page_id = this.root_page();
        super.insert(key, value);
        int new_page_id = this.root_page();
        if (new_page_id != old_page_id) {
            this.get_db().headers.set("schema_page", new_page_id);
        }
    }

    public void insert(String table_name, int root_page) {
        this.insert(
                Payload.create(
                        Arrays.asList(ObjType.STRING(DBFile.TABLE_NAME_LEN)),
                        Arrays.asList(table_name)
                ),
                Payload.create(
                        Arrays.asList(ObjType.INT),
                        Arrays.asList(root_page)
                )
        );
    }

    @Override
    public void delete(SearchResult sr) {
        int old_page_id = this.root_page();
        super.delete(sr);
        int new_page_id = this.root_page();
        if (new_page_id != old_page_id) {
            this.get_db().headers.set("schema_page", new_page_id);
        }
    }

    public SearchResult search(String table_name) {
        return this.search(Payload.create(
                Arrays.asList(ObjType.STRING(DBFile.TABLE_NAME_LEN)),
                Arrays.asList(table_name)
        ));
    }

    /**
     * get the root page number of the table.
     * @param table_name
     * @return 0 if not found.
     */
    public int get_table_root_page(String table_name) {
        SearchResult sr = this.search(table_name);
        if (sr.found()) {
            return this.get_value(sr).get_obj(0).as_int();
        }
        return 0;
    }

    public BTreeTable get_table(String table_name) {
        int root_page = this.get_table_root_page(table_name);
        if (root_page == 0) {
            throw new DBRuntimeError("table not found");
        }
        BTreeTable table = new BTreeTable(root_page, this.get_db(), table_name);
        return table;
    }
}
