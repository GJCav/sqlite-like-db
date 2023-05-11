package db;

import com.sun.xml.internal.ws.api.message.Header;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OverflowMgr {
    public static final List<FieldDef> HEADER_DEFS = new ArrayList() {{
        addAll(OverflowPage.HEADER_DEFS);
        add(new FieldDef(4, "cell_size", 0));
        add(new FieldDef(4, "cell_count", 0));
    }};

    private OverflowPage page;
    private Headers headers;

    public OverflowMgr(OverflowPage page) {
        this.page = page;
        this.headers = new Headers(HEADER_DEFS, page.page_id, page.owner);
    }

    public int get_cell_count() {
        return headers.get("cell_count").to_int();
    }

    public void set_cell_count(int cell_count) {
        headers.set("cell_count", cell_count);
    }

    public int get_cell_size() {
        return headers.get("cell_size").to_int();
    }

    public void set_cell_size(int cell_size) {
        headers.set("cell_size", cell_size);
    }

    public static OverflowMgr make_mgr(OverflowPage page) {
        OverflowPage.OutputStream out = page.get_output_stream();
        for (FieldDef def : HEADER_DEFS) {
            byte[] data = def.default_value;
            if (data.length < def.len) {
                byte[] temp = new byte[def.len];
                System.arraycopy(data, 0, temp, 0, data.length);
            }
            out.write(data);
        }
        return new OverflowMgr(page);
    }
}
