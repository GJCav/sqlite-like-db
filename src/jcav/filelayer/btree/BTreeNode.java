package jcav.filelayer.btree;

import jcav.filelayer.exception.DBRuntimeError;
import jcav.filelayer.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BTreeNode extends Page {
    public static List<FieldDef> BASIC_HDR_DEFS = new ArrayList<FieldDef>() {{
        add(new FieldDef(1, "type", PageType.BTREE_NULL));
        add(new FieldDef(4, "hdr_size", 0));
        add(new FieldDef(4, "father", 0));
        add(new FieldDef(4, "cell_size", 0));
        add(new FieldDef(4, "cell_count", 0)); // = free cell + payload cell
        add(new FieldDef(4, "free_cell", -1));
        add(new FieldDef(4, "key_count", 0));
        add(new FieldDef(4, "value_count", 0));
        //                                                                Interior       Leaf
        add(new FieldDef(4, "reserved1", 0));//     total          overflow_page
        add(new FieldDef(4, "reserved2", 0));//     tail_child     left_sibling
        add(new FieldDef(4, "reserved3", 0));//     not used       right_sibling
        // key_types, int[]
        // value_types, int[]

        // slot_capacity, int
        // slot_count, int, meaningful slot value count in slots
        // slots, int[], fixed length specified by slot_capacity
    }};

    ////////////////////////////////////////////////////////////
    // constructors
    ////////////////////////////////////////////////////////////

    protected BTreeNode(int page_id, DBFile owner) {
        super(page_id, owner);
        construct_headers(BASIC_HDR_DEFS);
    }

    public static BTreeNode create(int page_id, DBFile owner, List<FieldDef> basic_defs, int[] key_types, int[] val_types) {
        // latter we will modify the defs, so make a deepcopy here
        ArrayList<FieldDef> defs = basic_defs
                .stream()
                .map((v) -> (FieldDef)v.clone())
                .collect(Collectors.toCollection(ArrayList::new));

        FieldDef hdr_size_def = null;
        int cell_size = Cell.get_cell_size(key_types);
        for (FieldDef def : defs) {
            if (def.name.equals("hdr_size")) {
                hdr_size_def = def;
            } else if (def.name.equals("cell_size")) {
                def.default_value = Bytes.from_int(cell_size);
            } else if (def.name.equals("key_count")) {
                def.default_value = Bytes.from_int(key_types.length);
            } else if (def.name.equals("value_count")) {
                def.default_value = Bytes.from_int(val_types.length);
            }
        }

        defs.add(new FieldDef(key_types.length * 4, "key_types", key_types));
        defs.add(new FieldDef(val_types.length * 4, "value_types", val_types));

        int partial_hdr_size = Headers.get_total_length(defs) + 8; // slot_capacity, slot_count
        // partial_hdr_size + n * 4 + n * cell_size <= page_size
        // n * 4 is size for slots
        int slot_capacity = (owner.get_page_size(page_id) - partial_hdr_size) / (4 + cell_size);

        if (slot_capacity < 3) {
            throw new DBRuntimeError("keys are too large to fit in a page, " +
                    "please decrease key size or increase page size");
        }

        int hdr_size = partial_hdr_size + slot_capacity * 4;
        hdr_size_def.default_value = Bytes.from_int(hdr_size);

        defs.add(new FieldDef(4, "slot_capacity", slot_capacity));
        defs.add(new FieldDef(4, "slot_count", 0));
        defs.add(new FieldDef(slot_capacity * 4, "slots", new int[slot_capacity]));

        Headers hdr = new Headers(defs, page_id, owner);
        hdr.set_to_default();
        BTreeNode node = new BTreeNode(page_id, owner);
        node.headers = hdr;
        return node;
    }

    protected void construct_headers(List<FieldDef> basic_defs) {
        Headers basic_hdr = new Headers(basic_defs, page_id, owner);

        List<FieldDef> defs = new ArrayList<>(basic_defs);
        int key_count = basic_hdr.get("key_count").to_int();
        int value_count = basic_hdr.get("value_count").to_int();

        defs.add(new FieldDef(key_count * 4, "key_types", new int[key_count]));
        defs.add(new FieldDef(value_count * 4, "value_types", new int[value_count]));
        defs.add(new FieldDef(4, "slot_capacity", 0));
        defs.add(new FieldDef(4, "slot_count", 0));

        Headers partial_hdr = new Headers(defs, page_id, owner);
        int slot_capacity = partial_hdr.get("slot_capacity").to_int();

        defs.add(new FieldDef(slot_capacity * 4, "slots", new int[slot_capacity]));

        this.headers = new Headers(defs, page_id, owner);
    }

    ////////////////////////////////////////////////////////////
    // slot operations
    ////////////////////////////////////////////////////////////
    public int get_slot_capacity() {
        return headers.get("slot_capacity").to_int();
    }

    public int get_slot_count() {
        return headers.get("slot_count").to_int();
    }

    public List<Integer> get_slots() {
        int slot_count = get_slot_count();
        int[] slots = headers.get("slots").to_ints();
        return Arrays.stream(slots).boxed().collect(Collectors.toList()).subList(0, slot_count);
    }

    public int get_slot(int idx) {
        int slot_count = get_slot_count();
        if (idx < 0 || idx >= slot_count) {
            throw new IllegalArgumentException("slot index out of range");
        }
        return headers.get("slots").to_ints()[idx];
    }

    /**
     * this method will not corrupt consistence of slot_count
     * @param slots
     */
    public void set_slots(List<Integer> slots) {
        int slot_capacity = get_slot_capacity();
        if (slots.size() > slot_capacity) {
            throw new IllegalArgumentException("slots size exceeds capacity");
        }
        int[] slots_data = new int[slot_capacity];
        for (int i = 0; i < slots.size(); i++) {
            slots_data[i] = slots.get(i);
        }
        headers.set("slots", Bytes.from_ints(slots_data));
        headers.set("slot_count", Bytes.from_int(slots.size()));
    }

    /**
     * this method will not corrupt consistence of slot_count.
     * but cells that correspond to removed slot value will not be freed.
     * remember to free them manually to avoid space leak.
     *
     * @param idx
     * @return
     */
    public int remove_slot(int idx) {
        int slot_count = get_slot_count();
        if (idx < 0 || idx >= slot_count) {
            throw new IllegalArgumentException("slot index out of range");
        }
        List<Integer> slots = get_slots();
        int slot = slots.remove(idx);
        set_slots(slots);
        return slot;
    }


    ///////////////////////////////////////////////////////////
    // low level cell operations
    // for internal use only
    ///////////////////////////////////////////////////////////

    /**
     * the returned cell is dangling. make sure you will use
     * add this cell to a slot.
     *
     * @return
     */
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

        FreeCell cell = FreeCell.create(cell_id, get_cell_size());
        cell.set_next(next_free);
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

    protected void write_cell_data(int cell_id, byte[] data) {
        if (data.length != get_cell_size()) {
            throw new IllegalArgumentException("data length must be equal to cell_size");
        }
        int cell_offset = get_cell_offset(cell_id);
        write(cell_offset, data);
    }

    protected void set_free_cell(int cell_id) {
        headers.set("free_cell", cell_id);
    }

    protected void set_cell_count(int val) {
        headers.set("cell_count", val);
    }


    ////////////////////////////////////////////////////////////
    // getters
    ////////////////////////////////////////////////////////////

    /**
     * if this node is a leaf node, return the number of cells in this node.
     * if this node is an interior node, return the number of all keys in the subtree.
     * @return
     */
    public int get_total() {
        if (get_page_type() == PageType.BTREE_INTERIOR) {
            for(FieldDef def : headers.field_defs) {
                if (def.name.equals("total"))
                    return headers.get("total").to_int();
                else if (def.name.equals("reserved1"))
                    return headers.get("reserved1").to_int();
            }
            throw new RuntimeException("absurd situation");
        } else if (get_page_type() == PageType.BTREE_LEAF) {
            return get_slot_count();
        } else {
            throw new DBRuntimeError("page type " + get_page_type() + " do not supported get_total");
        }
    }

    /**
     * if this node is a leaf node, nothing will happen.
     * if this node is an interior node, set the number of all keys in the subtree.
     * @param total
     */
    public void set_total(int total) {
        if (get_page_type() == PageType.BTREE_INTERIOR) {
            for(FieldDef def : headers.field_defs) {
                if (def.name.equals("total"))
                    headers.set("total", total);
                else if (def.name.equals("reserved1"))
                    headers.set("reserved1", total);
            }
        } else if (get_page_type() == PageType.BTREE_LEAF) {
            return; // do nothing
        } else {
            throw new DBRuntimeError("page type " + get_page_type() + " do not supported set_total");
        }
    }

    public int get_key_count() {
        return headers.get("key_count").to_int();
    }

    public int[] get_key_types() {
        int key_count = get_key_count();
        byte[] data = headers.get("key_types").as_bytes();
        if (data.length != key_count * 4) {
            throw new DBRuntimeError("incomplete key_types field, expect "
                    + key_count * 4 + " bytes, got " + data.length);
        }
        int[] key_types = Bytes.to_ints(data);
        return key_types;
    }

    public List<Integer> get_key_type_list() {
        int[] key_types = get_key_types();
        List<Integer> key_type_list = new ArrayList<Integer>();
        for (int key_type : key_types) {
            key_type_list.add(key_type);
        }
        return key_type_list;
    }

    public int get_value_count() {
        return headers.get("value_count").to_int();
    }

    public int[] get_value_types() {
        int value_count = get_value_count();
        byte[] data = headers.get("value_types").as_bytes();
        if (data.length != value_count * 4) {
            throw new DBRuntimeError("incomplete value_types field, expect "
                    + value_count * 4 + " bytes, got " + data.length);
        }
        int[] value_types = Bytes.to_ints(data);
        return value_types;
    }

    public List<Integer> get_value_type_list() {
        int[] value_types = get_value_types();
        List<Integer> value_type_list = new ArrayList<Integer>();
        for (int value_type : value_types) {
            value_type_list.add(value_type);
        }
        return value_type_list;
    }

    public int get_free_cell() {
        return headers.get("free_cell").to_int();
    }

    public int get_body_size() {
        return owner.get_page_size(page_id) - get_page_header_size();
    }

    public int get_cell_count() {
        return headers.get("cell_count").to_int();
    }

    public int get_cell_size() {
        return headers.get("cell_size").to_int();
    }

    @Override
    public int get_page_header_size() {
        return headers.get("hdr_size").to_int();
    }


    public void set_father(int page_id) {
        headers.set("father", page_id);
    }

    public int get_father() {
        return headers.get("father").to_int();
    }


    public void _print_header() {
        headers.field_defs.forEach(def -> {
            System.out.println("- " + def.name + ": " + headers.get(def.name));
        });
    }
}
