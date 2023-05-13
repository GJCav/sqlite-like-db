package db.btree;

import db.Bytes;
import db.FieldDef;
import db.Headers;
import db.OverflowPage;
import db.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CellStorage {
    public static final List<FieldDef> EXT_HDR_DEFS = new ArrayList() {{
        add(new FieldDef(4, "unit_size", 0));
        add(new FieldDef(4, "unit_count", 0)); // used cell + free cell
        add(new FieldDef(4, "free_unit", -1));
        add(new FieldDef(4, "value_count", 0));
        // value_types, int[]
    }};

    private OverflowPage page;
    private Headers headers;

    public CellStorage(OverflowPage page) {
        if (page == null) {
            throw new NullPointerException("page is null");
        }

        this.page = page;
        construct_headers();
    }

    public static CellStorage create(OverflowPage page, int[] value_types) {
//        System.out.println("[CellStorage] create cell storage on page " + page.get_page_id());

        if (page == null) {
            throw new NullPointerException("page is null");
        }

        int unit_size = ObjType.get_size(value_types);
        unit_size = Math.max(unit_size, 4);            // at least 4 bytes to store next_free unit id

        List<FieldDef> defs = new ArrayList<>(OverflowPage.HEADER_DEFS);
        int start = defs.size();
        defs.add(new FieldDef(4, "unit_size", unit_size));
        defs.add(new FieldDef(4, "unit_count", 0));
        defs.add(new FieldDef(4, "free_unit", -1));
        defs.add(new FieldDef(4, "value_count", value_types.length));
        defs.add(new FieldDef(4 * value_types.length, "value_types", value_types));

        Headers hdr = new Headers(defs, page.get_page_id(), page.get_owner());
        for (int i = start;i < defs.size();i++) {
            FieldDef d = defs.get(i);
            hdr.set(d.name, d.default_value);
        }

        CellStorage storage = new CellStorage(page);
        storage.headers = hdr;

        return storage;
    }

    private void construct_headers() {
        List<FieldDef> defs = new ArrayList<>(OverflowPage.HEADER_DEFS);
        defs.addAll(EXT_HDR_DEFS);
        Headers partial = new Headers(defs, page.get_page_id(), page.get_owner());
        int value_count = partial.get("value_count").to_int();

        defs.add(new FieldDef(4 * value_count, "value_types", new int[value_count]));

        headers = new Headers(defs, page.get_page_id(), page.get_owner());
    }

    ///////////////////////////////////////////////////////////
    // unit management
    ///////////////////////////////////////////////////////////
    public int allocate_unit() {
        int free_unit = headers.get("free_unit").to_int();
        if (free_unit == -1) {
            int unit_count = headers.get("unit_count").to_int();
            headers.set("unit_count", unit_count + 1);
            OverflowPage.OutputStream out = get_unit_out_stream(unit_count);
            out.write(new byte[get_unit_size()]);
            return unit_count;
        } else {
            // find next free unit
            byte[] data = new byte[4];
            OverflowPage.InputStream in = get_unit_in_stream(free_unit);
            int sz = in.read(data);
            int next_free = Bytes.to_int(data);
            if (sz != 4) {
                throw new DBRuntimeError("CellStorage unit corrupted, incomplete next_free data, got " + sz + " bytes");
            }
            set_free_unit(next_free);

            // clear the unit
            OverflowPage.OutputStream out = get_unit_out_stream(free_unit);
            out.write(new byte[get_unit_size()]);
            return free_unit;
        }
    }

    public void release_unit(int unit_id) {
        int next_free = headers.get("free_unit").to_int();

        OverflowPage.OutputStream out = get_unit_out_stream(unit_id);
        out.write(Bytes.from_int(next_free));
        set_free_unit(unit_id);
    }


    ///////////////////////////////////////////////////////////
    // IO methods
    ///////////////////////////////////////////////////////////

    /**
     * pos in OverflowPage InputStream/OutputStream
     * @return
     */
    private long get_unit_pos(int cell_id) {
        int unit_size = headers.get("unit_size").to_int();
        long offset = headers.get_total_length() - Headers.get_total_length(OverflowPage.HEADER_DEFS)
                + unit_size * cell_id;
        return offset;
    }

    /**
     * DO NOT WRITE data that exceeds unit_size, it will corrupt the page
     * @param unit_id
     * @return
     */
    public OverflowPage.OutputStream get_unit_out_stream(int unit_id) {
        long offset = get_unit_pos(unit_id);
        return page.get_output_stream(offset);
    }

    /**
     * DO NOT READ data that exceeds unit_size, it is undefined
     * @param unit_id
     * @return
     */
    public OverflowPage.InputStream get_unit_in_stream(int unit_id) {
        long offset = get_unit_pos(unit_id);
        return page.get_input_stream(offset);
    }

    public Payload get_unit(int unit_id) {
        byte[] data = new byte[get_unit_size()];
        OverflowPage.InputStream in = get_unit_in_stream(unit_id);
        int sz = in.read(data);
        if (sz != data.length) {
            throw new DBRuntimeError("CellStorage unit corrupted, incomplete unit data");
        }
        Payload payload = new Payload(get_value_type_list(), data);
        return payload;
    }

    public void set_unit(int unit_id, Payload payload) {
        List<Integer> value_types = get_value_type_list();
        if (!Payload.is_compatible(value_types, payload.get_types())) {

            throw new DBRuntimeError("Payload is not compatible with this CellStorage, " +
                    "expected " + ObjType.to_string(value_types) +
                    ", got " + ObjType.to_string(payload.get_types()));
        }

        byte[] data = payload.get_bytes();
        OverflowPage.OutputStream out = get_unit_out_stream(unit_id);
        out.write(data);
    }


    //////////////////////////////////////////////////////////
    // setters
    //////////////////////////////////////////////////////////
    private void set_free_unit(int free_unit) {
        headers.set("free_unit", free_unit);
    }


    ///////////////////////////////////////////////////////////
    // getters
    ///////////////////////////////////////////////////////////
    public int get_unit_size() {
        return headers.get("unit_size").to_int();
    }

    public int get_unit_count() {
        return headers.get("unit_count").to_int();
    }

    public int get_free_unit() {
        return headers.get("free_unit").to_int();
    }

    public int get_value_count() {
        return headers.get("value_count").to_int();
    }

    public int[] get_value_types() {
        return headers.get("value_types").to_ints();
    }

    public List<Integer> get_value_type_list() {
        return Arrays.stream(get_value_types()).boxed().collect(Collectors.toList());
    }

    public void _print_headers() {
        System.out.println("Storage at overflow page " + page.get_page_id());

        for (FieldDef d : headers.field_defs) {
            System.out.println(d.name + ": " + headers.get(d.name));
        }
    }
}
