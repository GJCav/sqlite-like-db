package db;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Page {
    public static final List<FieldDef> HEADER_DEFS = Arrays.asList(
            new FieldDef(1, "type", PageType.NULL)
    );

    protected int page_id;
    protected DBFile owner;
    protected Headers headers;

    public Page(int page_id, DBFile owner) {
        this.page_id = page_id;
        this.owner = owner;
        this.headers = new Headers(HEADER_DEFS, page_id, owner);
    }

    public int get_page_header_size() {
        return headers.get_total_length();
    }

    public int get_page_id() { return page_id; }

    public byte get_page_type() { return headers.get("type").to_byte(); }

    public Headers get_headers() { return headers; }

    public DBFile get_owner() { return owner; }

    /**
     * ATTENTION: this will read page headers.
     * @param pos
     * @param length
     * @return
     * @throws IOException
     */
    public byte[] read(int pos, int length) {
        return owner.read(page_id, pos, length);
    }


    public void write(int pos, byte[] data) {
        owner.write(page_id, pos, data);
    }

    /**
     * ATTENTION: this can write on page headers.
     * @param pos
     * @param data
     * @param offset
     * @param length
     */
    public void write(int pos, byte[] data, int offset, int length) {
        owner.write(page_id, pos, data, offset, length);
    }

    public void init_page() {
        headers.set_to_default();
    }
}
