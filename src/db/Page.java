package db;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Page {
    public static final byte TYPE_NULL = 0;
    public static final byte TYPE_FREE = 1;
    public static final List<FieldDef> HEADER_DEFS = Arrays.asList(
            new FieldDef(1, "type", TYPE_NULL)
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

    public Headers get_headers() { return headers; }
}
