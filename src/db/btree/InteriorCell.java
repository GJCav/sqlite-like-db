package db.btree;

import db.Bytes;
import db.FieldDef;
import db.Headers;
import db.exception.DBRuntimeError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InteriorCell extends Cell {
    public static final List<FieldDef> HEADER_DEFS = new ArrayList() {{
        add(new FieldDef(1, "type", CellType.INTERIOR));
        add(new FieldDef(4, "child_page", 0));
    }};

    private List<Integer> payload_types = new ArrayList<>();

    public InteriorCell(int cell_id, byte[] data, List<Integer> payload_types) {
        super(cell_id, data);
        if (payload_types == null) throw new NullPointerException("payload_types must not be null");

        header_defs = HEADER_DEFS;
        this.payload_types = payload_types;

        int exp_size = get_header_size() + ObjType.get_size(payload_types);
        if (data.length != exp_size)
            throw new DBRuntimeError("data size mismatch, expect " + exp_size
                    + " bytes, got " + data.length + " bytes");
    }

    public InteriorCell(int cell_id, byte[] data, int[] payload_types) {
        super(cell_id, data);
        if (payload_types == null) throw new NullPointerException("payload_types must not be null");

        header_defs = HEADER_DEFS;
        this.payload_types = Arrays.stream(payload_types).boxed().collect(Collectors.toList());

        int exp_size = get_header_size() + ObjType.get_size(payload_types);
        if (data.length != exp_size)
            throw new DBRuntimeError("data size mismatch, expect " + exp_size
                    + " bytes, got " + data.length + " bytes");
    }

    public static InteriorCell create(int cell_id, byte data[], int[] payload_types) {
        data[0] = CellType.INTERIOR;
        InteriorCell cell = new InteriorCell(cell_id, data, payload_types);
        return cell;
    }

    public static InteriorCell create(int cell_id, int[] payload_types) {
        return create(
                cell_id,
                new byte[Headers.get_total_length(HEADER_DEFS) + ObjType.get_size(payload_types)],
                payload_types
        );
    }

    public int get_child() {
        int page = Bytes.to_int(data, 1);
        return page;
    }

    public void set_child(int page) {
        byte[] val = Bytes.from_int(page);
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
