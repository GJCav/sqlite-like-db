package jcav.filelayer.btree;

import jcav.filelayer.FieldDef;
import jcav.filelayer.Headers;
import jcav.filelayer.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.List;

/**
 * This class always stores data in memory
 */
public abstract class Cell {
    protected List<FieldDef> header_defs = new ArrayList() {{
        add(new FieldDef(1, "type", CellType.INTERIOR));
        add(new FieldDef(4, "reserved", 0));
    }};
    protected byte[] data = null;
    public int cell_id = -1; // -1 means null

    /**
     * make sure no other object can modify the data
     *
     * @param cell_id
     * @param data
     */
    public Cell(int cell_id, byte[] data) {
        if (data == null) throw new NullPointerException("data must not be null");
        this.cell_id = cell_id;
        if (data.length < get_header_size()) {
            throw new DBRuntimeError("data too short to be an abstract cell");
        }
        this.data = data;
    }

    public byte get_type() { return data[0]; }

    public int get_header_size() {
        return header_defs.stream().mapToInt(h -> h.len).sum();
    }

    public byte[] get_data() { return data; }

    public static int get_cell_size(int[] key_types) {
        return 5 + Payload.get_size(key_types);
    }

    public static int get_cell_size(List<Integer> key_types) {
        return 5 + Payload.get_size(key_types);
    }
}
