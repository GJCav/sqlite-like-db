package jcav.filelayer;

import jcav.filelayer.btree.BPlusTree;
import jcav.filelayer.btree.ObjType;
import jcav.filelayer.exception.DBRuntimeError;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class DBFile implements Closeable {
    public static final int HEADER_SIZE = 128;
    public static final int TABLE_NAME_LEN = 64;
    public static final List<FieldDef> HEADER_DEFS = Arrays.asList(
            new FieldDef(32, "file_id", "SQLite-like-db"),
            new FieldDef(2, "ver", (short) 1),
            new FieldDef(1, "page_size", (byte) 12), // 512 byte page size, for debugging
            new FieldDef(4, "page_count", 1),
            new FieldDef(4, "freelist_head", 0),
            new FieldDef(4, "freelist_count", 0),
            new FieldDef(4, "cache_count", 128),
            new FieldDef(4,"schema_page", 0)
    );

    public DBFile(String path) throws IOException {
        this.path = path;
        cache = new NoCache(this);
        headers = new Headers(HEADER_DEFS, 0, this);

        recover();
    }

    private void recover() {
        String wal_path = path + ".wal";
        if (!Files.exists(Paths.get(wal_path))) return;
        Transaction tx = new Transaction(this);
        tx.recover();
    }

    ////////////////////////////
    // Headers
    ////////////////////////////
    protected Headers headers;

    public Headers get_headers() {
        return headers;
    }

    ////////////////////////////
    // Page
    ////////////////////////////

    /**
     * Allocate a new page, or reuse a free page.
     * The page data is initialized to all 0.
     * @return
     */
    public int alloc_page() {
        int free_page = headers.get("freelist_head").to_int();
        if (free_page != 0) {
            // reuse a free page
            FreePage free = new FreePage(free_page, this);
            headers.set("freelist_head", free.get_next_free());
            headers.set("freelist_count", headers.get("freelist_count").to_int() - 1);
            write(free_page, 0, new byte[get_page_size(free_page)]);
            return free_page;
        } else {
            // allocate a new page

            try {
                int page_count = headers.get("page_count").to_int();
                if (transaction != null) {
                    // Special case: allocate a new page in a transaction
                    // this is required because the following this.write will make the transaction
                    // read the page at page_count, which is not allocated yet and will cause an
                    // incomplete page error.
                    transaction.notify_alloc_page(page_count);
                }
                this.write(page_count, 0, new byte[get_page_size(page_count)]);
                headers.set("page_count", page_count + 1);
                return page_count;
            } catch (Exception e) {
                throw new DBRuntimeError("Failed to allocate new page", e);
            }
        }
    }

    /**
     * Release a page, add it to the free list.
     * Only set the page type to FREE, and set the next_free pointer. The page body is left unchanged.
     * @param page_id
     */
    public void release_page(int page_id) {
        FreePage free = new FreePage(page_id, this);
        free.headers.set("type", PageType.FREE);
        free.headers.set("next_free", headers.get("freelist_head"));
        headers.set("freelist_head", page_id);
        headers.set("freelist_count", headers.get("freelist_count").to_int() + 1);
    }

    ////////////////////////////
    // IO & cache & transaction
    ////////////////////////////
    protected String path;
    protected Cache cache;
    protected boolean _readonly = false;
    protected Transaction transaction = null;

    /**
     * set the cache, close the old cache.
     * @param cache
     * @throws DBRuntimeError
     */
    public void set_cache(Cache cache) {
        try {
            this.cache.close();
        } catch (IOException e) {
            throw new DBRuntimeError("Failed to close old cache", e);
        }
        this.cache = cache;
    }

    public void use_LRUCache() {
        set_cache(new LRUCache(this, headers.get("cache_count").to_int()));
    }

    public Transaction transaction() {
        if (transaction != null) {
            throw new DBRuntimeError("Transaction already exists");
        }
        transaction = Transaction.create(this);
        return transaction;
    }

    /**
     * read some data from the cache, the returned byte array is a copy of the
     * data in the cache. So it is safe to modify it, but remember to write it
     * back.
     *
     * the underlying function is {@link Cache#read(int, int, int)}
     *
     * @param page_id
     * @param pos
     * @param length
     * @return
     */
    public byte[] read(int page_id, int pos, int length) {
        if (transaction == null)
            return this.cache.read(page_id, pos, length);
        else
            return transaction.read(page_id, pos, length);
    }

    /**
     * @see #write(int, int, byte[], int, int)
     *
     * @param page_id
     * @param pos
     * @param data
     * @param offset
     * @param length
     */
    public void write(int page_id, int pos, byte[] data, int offset, int length) {
        if (_readonly) throw new DBRuntimeError("DBFile is readonly");
        if (transaction == null)
            this.cache.write(page_id, pos, data, offset, length);
        else
            transaction.write(page_id, pos, data, offset, length);
    }

    /**
     * write some data to cache
     *
     * @param page_id
     * @param pos position relative to the start of the page
     * @param data
     */
    public void write(int page_id, int pos, byte[] data) {
        if (_readonly) throw new DBRuntimeError("DBFile is readonly");
        if (transaction == null) {
            this.cache.write(page_id, pos, data, 0, data.length);
        } else {
            transaction.write(page_id, pos, data, 0, data.length);
        }
    }

    /**
     * sync the cache and close the file.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.cache.sync();
        this.cache.close();
    }

    /**
     * sync the cache.
     * @throws IOException
     */
    public void sync() throws IOException {
        this.cache.sync();
    }


    ////////////////////////////
    // helpful functions
    ////////////////////////////
    public long get_page_offset(int page_id) {
        if (page_id < 0) throw new IllegalArgumentException("page_id must be positive");
        if(page_id == 0) return 0;
        return HEADER_SIZE + (page_id - 1L) * get_page_size(page_id);
    }

    public int get_page_size(int page_id) {
        if (page_id < 0) throw new IllegalArgumentException("page_id must be positive");
        if(page_id == 0) return HEADER_SIZE;
        int m = headers.get("page_size").as_byte();
        return 1<<m;
    }

    public SchemaTable get_schema() {
        int page_id = headers.get("schema_page").to_int();
        if (page_id == 0) {
            throw new DBRuntimeError("schema page not set");
        }
        return new SchemaTable(page_id, this);
    }

    ////////////////////////////
    // db create
    ////////////////////////////
    public static DBFile create(String path) throws IOException, DBRuntimeError {
        DBFile db = new DBFile(path);
        db.headers.set_to_default();
        int pos = db.headers.get_total_length();
        db.write(0, pos, new byte[DBFile.HEADER_SIZE - pos]);
        db.sync();

        db.set_cache(new LRUCache(db, db.headers.get("cache_count").to_int()));

        int page_id = db.alloc_page();
        db.headers.set("schema_page", page_id);
        BPlusTree.create(
                page_id,
                db,
                Arrays.asList(ObjType.STRING(TABLE_NAME_LEN)),
                //            root_page
                Arrays.asList(ObjType.INT)
        );
        return db;
    }
}
