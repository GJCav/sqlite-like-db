package jcav.filelayer;

import jcav.filelayer.btree.*;
import jcav.filelayer.exception.DBRuntimeError;

import java.util.Arrays;
import java.util.List;

public class BTreeTable extends BPlusTree {
    private String table_name = "";

    protected BTreeTable(int root_page, DBFile db, String name) {
        super(root_page, db);
        table_name = name;
    }

    private void update_root(int new_root) {
        SchemaTable st = this.get_db().get_schema();
        SearchResult sr = st.search(table_name);
        if(!sr.found()) {
            throw new DBRuntimeError("table not found in schema");
        }
        st.set_value(
                sr,
                Payload.create(Arrays.asList(ObjType.INT), Arrays.asList(new_root))
        );
    }

    @Override
    public void insert(Payload key, Payload value) {
        int root_page = this.root_page();
        super.insert(key, value);
        int new_root = this.root_page();
        if (new_root != root_page) {
            update_root(new_root);
        }
    }

    @Override
    public void delete(SearchResult sr) {
        int root_page = this.root_page();
        super.delete(sr);
        int new_root = this.root_page();
        if (new_root != root_page) {
            update_root(new_root);
        }
    }

    public void drop_self() {
        SchemaTable st = this.get_db().get_schema();
        SearchResult sr = st.search(table_name);
        if(!sr.found()) {
            throw new DBRuntimeError("table not found in schema, this is an orphan table");
        }
        st.delete(sr);

        release_self();
    }

    public static BTreeTable create(
            DBFile db,
            String table_name,
            List<Integer> key_types,
            List<Integer> val_types
    ) {
        int page_id = db.alloc_page();
        BPlusTree tree = BPlusTree.create(page_id, db, key_types, val_types);
        SchemaTable schema = db.get_schema();
        schema.insert(table_name, page_id);

        BTreeTable table = new BTreeTable(page_id, db, table_name);
        return table;
    }
}
