package db;

import java.util.ArrayList;
import java.util.List;

public class BTreeNode extends Page {
    public static List<FieldDef> BASIC_HEADERS = new ArrayList<FieldDef>() {{
        add(new FieldDef(1, "type", PageType.BTREE_NULL));
        add(new FieldDef(4, "hdr_size", 0));
        add(new FieldDef(4, "father", 0));
        add(new FieldDef(4, "key_count", 0));
        add(new FieldDef(4, "cell_size", 0));
        add(new FieldDef(4, "cell_count", 0));
        add(new FieldDef(4, "free_cell", -1));
        add(new FieldDef(4, "key_count", 0));
        add(new FieldDef(4, "value_count", 0));
        //                                                                Interior       Leaf
        add(new FieldDef(4, "reserved1", 0));//     total          overflow_page
        add(new FieldDef(4, "reserved2", 0));//     tail_child     left_sibling
        add(new FieldDef(4, "reserved3", 0));//     not used       right_sibling
        // key_types
        // value_types
    }};

    public BTreeNode(int page_id, DBFile owner) {
        super(page_id, owner);
        throw new RuntimeException("DO NOT USE THIS CONSTRUCTOR");
    }

    protected int allocate_cell() {
        int cell_size = get_cell_size();

        // reuse free cells
        int free_cell = get_free_cell();
        if (free_cell != -1) {
            byte[] cell_data = read_cell_data(free_cell);
            FreeCell cell = new FreeCell(free_cell, cell_data);
            int next_id = cell.get_next();
            set_free_cell(next_id);
            // clear cell data
            write_cell_data(free_cell, new byte[cell_size]);
            return free_cell;
        }

        // allocate new cell
        int body_size = get_body_size();
        int available_size = body_size - get_cell_count() * get_cell_size();
        if (available_size < cell_size) {
            return -1;
        }
        int cell_id = get_cell_count();
        write_cell_data(cell_id, new byte[cell_size]);
        set_cell_count(cell_id + 1);
        return cell_id;
    }

    protected void check_cell_id(int cell_id) {
        if (cell_id < 0) throw new IllegalArgumentException("cell_id must be positive");
        int cell_size = get_cell_size();
        int cell_offset = get_cell_offset(cell_id);
        if (cell_offset + cell_size > owner.get_page_size(page_id)) {
            throw new IllegalArgumentException("cell_id out of range");
        }
    }

    protected void release_cell(int cell_id) {
        check_cell_id(cell_id);

        int next_free = get_free_cell();
        FreeCell cell = FreeCell.create(next_free, get_cell_size());
        byte[] cell_data = cell.get_data();
        write_cell_data(cell_id, cell_data);
        set_free_cell(cell_id);
    }

    protected int get_cell_offset(int cell_id) {
        return get_page_header_size() + cell_id * get_cell_size();
    }

    protected byte[] read_cell_data(int cell_id) {
        check_cell_id(cell_id);
        int cell_offset = get_cell_offset(cell_id);
        int cell_size = get_cell_size();
        byte[] cell_data = read(cell_offset, cell_size);
        return cell_data;
    }

    public void write_cell_data(int cell_id, byte[] data) {
        if (data.length != get_cell_size()) {
            throw new IllegalArgumentException("data length must be equal to cell_size");
        }
        int cell_offset = get_cell_offset(cell_id);
        write(cell_offset, data);
    }


    public int get_free_cell() {
        return headers.get("free_cell").to_int();
    }

    protected void set_free_cell(int cell_id) {
        headers.set("free_cell", cell_id);
    }

    public int get_body_size() {
        return owner.get_page_size(page_id) - get_page_header_size();
    }

    public int get_cell_count() {
        return headers.get("cell_count").to_int();
    }

    protected void set_cell_count(int val) {
        headers.set("cell_count", val);
    }

    public int get_cell_size() {
        return headers.get("cell_size").to_int();
    }

    @Override
    public int get_page_header_size() {
        return headers.get("hdr_size").to_int();
    }

    @Override
    public void init_page() {
        throw new RuntimeException("not supported");
    }
}
