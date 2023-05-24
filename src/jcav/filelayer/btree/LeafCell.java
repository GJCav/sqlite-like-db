package jcav.filelayer.btree;

import jcav.filelayer.Bytes;
import jcav.filelayer.FieldDef;
import jcav.filelayer.Headers;
import jcav.filelayer.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LeafCell extends Cell {
    public static final List<FieldDef> HEADERS = new ArrayList() {{
        add(new FieldDef(1, "type", CellType.LEAF));
        add(new FieldDef(4, "unit_id", 0));
    }};

    private List<Integer> payload_types = new ArrayList<>();

    /**
     * make sure no other object can modify the data
     *
     * @param cell_id
     * @param data
     */
    public LeafCell(int cell_id, byte[] data, List<Integer> payload_types) {
        super(cell_id, data);
        if (payload_types == null) throw new NullPointerException("payload_types must not be null");

        header_defs = HEADERS;
        this.payload_types = payload_types;

//        int exp_size = get_header_size() + ObjType.get_size(payload_types);
        int exp_size = get_cell_size(payload_types);
        if (data.length != exp_size)
            throw new DBRuntimeError("data size mismatch, expect " + exp_size
                    + " bytes, got " + data.length + " bytes");
    }

    public LeafCell(int cell_id, byte[] data, int[] types) {
        super(cell_id, data);
        if (payload_types == null) throw new NullPointerException("payload_types must not be null");

        header_defs = HEADERS;
        this.payload_types = Arrays.stream(types).boxed().collect(Collectors.toList());

//        int exp_size = get_header_size() + ObjType.get_size(payload_types);
        int exp_size = get_cell_size(types);
        if (data.length != exp_size)
            throw new DBRuntimeError("data size mismatch, expect " + exp_size
                    + " bytes, got " + data.length + " bytes");
    }

    public static LeafCell create(int cell_id, byte[] data, int[] key_types) {
        data[0] = CellType.LEAF;
        LeafCell cell = new LeafCell(cell_id, data, key_types);
        return cell;
    }

    public static LeafCell create(int cell_id, int[] key_types) {
        return create(
                cell_id,
                new byte[get_cell_size(key_types)],
                key_types
        );
    }


    public int get_unit_id() {
        int val = Bytes.to_int(data, 1);
        return val;
    }

    public void set_unit_id(int id) {
        byte[] val = Bytes.from_int(id);
        System.arraycopy(val, 0, data, 1, val.length);
    }

    public Payload get_key() {
        int offset = get_header_size();
        int size = data.length - offset;
        byte[] key_data = new byte[size];
        System.arraycopy(data, offset, key_data, 0, size);
        return new Payload(payload_types, key_data);
    }

    public void set_key(Payload key) {
        int offset = get_header_size();
        byte[] key_data = key.get_bytes();
        if (key_data.length != data.length - offset)
            throw new DBRuntimeError("key size mismatch, expected " + (data.length - offset)
                    + " bytes, got " + key_data.length + " bytes");
        System.arraycopy(key_data, 0, data, offset, key_data.length);
    }
}
