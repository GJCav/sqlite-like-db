package jcav.filelayer;

import jcav.filelayer.btree.ObjType;
import jcav.filelayer.btree.Payload;
import jcav.filelayer.btree.SearchResult;
import jcav.filelayer.exception.DBRuntimeError;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

public class Transaction implements Closeable {
    public static final int W_ROLLBACK = 0;
    public static final int W_COMMITTING = 1;
    public static final int W_COMMITTED = 2;
    public static final int W_CLOSED = -1;
    public static final FieldDef STATE_DEF = new FieldDef(4, "w_state", W_ROLLBACK);

    protected DBFile db;
    protected DBFile wal_db;
    private boolean is_committed = false;
    private boolean is_closed = false;
    private boolean commit_on_close = false;
    protected BTreeTable records = null;

    protected Transaction(DBFile db) {
        this.db = db;
        try {
            wal_db = new DBFile(db.path + ".wal");
            wal_db.use_LRUCache();
        } catch (IOException e) {
            throw new DBRuntimeError("create WAL error", e);
        }
        wal_db.headers.field_defs = new ArrayList<>(db.headers.field_defs);
        wal_db.headers.field_defs.add(STATE_DEF);
        wal_db.set_cache(new LRUCache(
                wal_db,
                wal_db.get_headers().get("cache_count").to_int()
        ));
        records = wal_db.get_schema().get_table("records");
    }

    protected static Transaction create(DBFile db) {
        String path = db.path + ".wal";
        if (Files.exists(Paths.get(path))) {
            throw new DBRuntimeError("WAL file already exists: " + path + ". Consider to recover it first.");
        }

        try (DBFile wal_db = DBFile.create(path)) {
            BTreeTable.create(
                    wal_db,
                    "records",
                    Arrays.asList(ObjType.INT), // page num
                    /**
                     * old data page, new data page
                     *       0      ,       1
                     */
                    Arrays.asList(ObjType.INT, ObjType.INT)
            );
        } catch (IOException e) {
            throw new DBRuntimeError("Unable to create WAL file: " + path, e);
        }

        return new Transaction(db);
    }

    private void _check() {
        if (wal_db == null) {
            throw new DBRuntimeError("this transaction has already been closed.");
        }
    }

    private int get_ndp(int page_id) {
        SearchResult sr = records.search(Payload.create(
                Arrays.asList(ObjType.INT),
                Arrays.asList(page_id)
        ));
        int ndp = 0;
        if (sr.found()) {
            ndp = sr.get_value().get_obj(1).as_int();
        }
        return ndp;
    }

    private int[] make_record(int page_id) {
        if (get_ndp(page_id) != 0) {
            throw new RuntimeException("this shouldn't happen");
        }
        int odp = wal_db.alloc_page();
        int ndp = wal_db.alloc_page();

        byte[] old_data = db.cache.read(page_id, 0, db.get_page_size(page_id));
        wal_db.write(odp, 0, old_data);
        wal_db.write(ndp, 0, old_data);

        records.insert(
                Payload.create(Arrays.asList(ObjType.INT), Arrays.asList(page_id)),
                Payload.create(Arrays.asList(ObjType.INT, ObjType.INT), Arrays.asList(odp, ndp))
        );
        return new int[]{odp, ndp};
    }

    public void notify_alloc_page(int new_page) {
        int odp = wal_db.alloc_page();
        int ndp = wal_db.alloc_page();
        byte[] old_data = new byte[db.get_page_size(new_page)];
        wal_db.write(odp, 0, old_data);
        wal_db.write(ndp, 0, old_data);
        records.insert(
                Payload.create(Arrays.asList(ObjType.INT), Arrays.asList(new_page)),
                Payload.create(Arrays.asList(ObjType.INT, ObjType.INT), Arrays.asList(odp, ndp))
        );
    }

    public void write(int page_id, int pos, byte[] data) {
        _check();
        int ndp = get_ndp(page_id);
        if (ndp == 0) {
            ndp = make_record(page_id)[1];
        }
        wal_db.write(ndp, pos, data);
    }

    public void write(int page_id, int pos, byte[] data, int offset, int length) {
        _check();
        int ndp = get_ndp(page_id);
        if (ndp == 0) {
            ndp = make_record(page_id)[1];
        }
        wal_db.write(ndp, pos, data, offset, length);
    }

    public byte[] read(int page_id, int pos, int length) {
        _check();
        int ndp = get_ndp(page_id);
        byte[] data = null;
        if (ndp == 0) {
            data = db.cache.read(page_id, pos, length);
        } else {
            data = wal_db.read(ndp, pos, length);
        }
        return data;
    }

    public void write_back() {
        if(wal_db.headers.get("w_state").to_int() != W_COMMITTING) {
            throw new DBRuntimeError("write back is not allowed before committing");
        }
        records.foreach_leaf(leaf -> {
            int count = leaf.get_slot_count();
            for (int i = 0;i < count;i++) {
                Payload key = leaf.get_key(i);
                Payload value = leaf.get_value(i);
                int page_id = key.get_obj(0).as_int();
//                int odp = value.get_obj(0).as_int();
                int ndp = value.get_obj(1).as_int();
                byte[] new_data = wal_db.read(ndp, 0, db.get_page_size(page_id));
                db.cache.write(page_id, 0, new_data);
            }
        });
    }

    protected void recover() {
        int w_state = wal_db.headers.get("w_state").to_int();
        if (w_state == W_ROLLBACK) {
            rollback();
        } else if (w_state == W_COMMITTING) {
            commit();
        } else {
            throw new DBRuntimeError("unknown w_state: " + w_state);
        }
    }

    public void commit() {
        _check();
        if (is_committed) {
            throw new DBRuntimeError("unable to commit a committed transaction");
        }
        try {
            wal_db.headers.set("w_state", W_COMMITTING);
            wal_db._readonly = true;
            wal_db.sync();
        } catch (IOException e) {
            throw new DBRuntimeError("commit error", e);
        }
        write_back();
        is_committed = true;
        del_wal();
    }

    public void rollback() {
        _check();
        if (is_committed) {
            throw new DBRuntimeError("unable to rollback a committed transaction");
        }
        del_wal();
    }

    private void del_wal() {
        try {
            wal_db.close();
            String path = wal_db.path;
            Files.delete(Paths.get(path));
        } catch (IOException e) {
            throw new DBRuntimeError("delete WAL error", e);
        }
        db.transaction = null;
        wal_db = null;
        db = null;
    }

    public void commit_on_close() {
        _check();
        commit_on_close = true;
    }

    public int get_status() {
        if (is_closed) return W_CLOSED;
        if (wal_db == null) return W_CLOSED;
        int status = wal_db.headers.get("w_state").to_int();
        return status;
    }

    @Override
    public void close() throws IOException {
        if (wal_db == null) return; // already closed
        try {
            if (!is_committed && commit_on_close) {
                commit();
            }
            if (!is_committed) {
                rollback();
            }
        } finally {
            is_closed = true;
        }
    }
}
