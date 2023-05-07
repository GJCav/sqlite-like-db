package db;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FreePage extends Page {
    public static final List<FieldDef> HEADER_DEFS = Arrays.asList(
            new FieldDef(1, "type", TYPE_FREE),
            new FieldDef(4, "next_free", 0)
    );

    public FreePage(int page_id, DBFile owner) {
        super(page_id, owner);
        headers = new Headers(HEADER_DEFS, page_id, owner);
    }

    public int get_next_free() throws IOException {
        return headers.get("next_free").to_int();
    }
}
