package db;

import db.exception.DBRuntimeError;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * payload always stores data in memory
 */
public class Payload {
    private List<Integer> types = new ArrayList<>();
    private byte[] data = null;


    /**
     * make sure no other object can modify the data
     *
     * @param types
     * @param data
     */
    public Payload(List<Integer> types, byte[] data) {
        if (types == null) throw new NullPointerException("types must not be null");
        if (data == null) throw new NullPointerException("data must not be null");
        this.types = new ArrayList<>(types);
    }

    public int get_obj_count() {
        return types.size();
    }

    public int get_obj_size(int idx) {
        return ObjType.get_size(types.get(idx));
    }

    private int get_obj_offset(int idx) {
        int offset = 0;
        for (int i = 0; i < idx; i++) {
            offset += ObjType.get_size(types.get(i));
        }
        return offset;
    }

    public int get_obj_type(int idx) {
        return types.get(idx);
    }

    public ObjValue get_obj(int idx) {
        int type = types.get(idx);
        int offset = get_obj_offset(idx);
        int size = get_obj_size(idx);

        if (data.length <= offset + size)
            throw new DBRuntimeError("data too short");

        Object val = null;
        if (type == ObjType.INT) {
            val = Bytes.to_int(data, offset);
        } else if (type == ObjType.LONG) {
            val = Bytes.to_long(data, offset);
        }else if (type == ObjType.FLOAT) {
            val = Bytes.to_float(data, offset);
        } else if (ObjType.is_type_string(type)) {
            val = Bytes.to_string(data, offset, size);
        } else {
            throw new DBRuntimeError("unknown type " + ObjType.to_string(type));
        }
        return new ObjValue(type, val);
    }

    public void set_obj(int idx, ObjValue val) {
        if (idx < 0 || idx >= types.size())
            throw new IndexOutOfBoundsException("index out of range, idx = " + idx + ", size = " + types.size());
        if (val == null) throw new NullPointerException("val must not be null");

        int type = types.get(idx);
        if (type != val.type)
            throw new IllegalArgumentException("type mismatch, obj type is " + ObjType.to_string(val.type)
                    + ", target type is " + ObjType.to_string(type));

        int offset = get_obj_offset(idx);
        int size = get_obj_size(idx);

        if (data.length <= offset + size)
            throw new DBRuntimeError("data too short");

        if (type == ObjType.INT) {
            byte[] val_bytes = Bytes.from_int(val.as_int());
            System.arraycopy(val_bytes, 0, data, offset, size);
        } else if (type == ObjType.LONG) {
            byte[] val_bytes = Bytes.from_long(val.as_long());
            System.arraycopy(val_bytes, 0, data, offset, size);
        } else if (type == ObjType.FLOAT) {
            byte[] val_bytes = Bytes.from_float(val.as_float());
            System.arraycopy(val_bytes, 0, data, offset, size);
        } else if (ObjType.is_type_string(type)) {
            byte[] str_bytes = Bytes.from_string(val.as_string());
            System.arraycopy(str_bytes, 0, data, offset, str_bytes.length);
            if (str_bytes.length < size) {
                Arrays.fill(data, offset + str_bytes.length, offset + size, (byte) 0);
            }
        } else {
            throw new DBRuntimeError("unknown type " + ObjType.to_string(type));
        }
    }

    public int get_payload_size() {
        int sz = 0;
        for (int t : types) {
            sz += ObjType.get_size(t);
        }
        return sz;
    }

    public byte[] get_bytes() {
        return data;
    }

    public static ObjValue wrap_object(int val) {
        return new ObjValue(ObjType.INT, val);
    }

    public static ObjValue wrap_object(long val) {
        return new ObjValue(ObjType.LONG, val);
    }

    public static ObjValue wrap_object(float val) {
        return new ObjValue(ObjType.FLOAT, val);
    }

    public static ObjValue wrap_object(int len, String val) {
        if (len < val.length()) throw new IllegalArgumentException("len must be greater than val.length()");
        int type = ObjType.STRING(len);
        return new ObjValue(type, val);
    }

    public static final class ObjValue {
        private int type;
        private Object obj;

        public ObjValue(int type, Object obj) {
            this.type = type;
            this.obj = obj;
        }

        public int get_type() {
            return type;
        }

        public Object get_obj() {
            return obj;
        }

        public int as_int() {
            if (type != ObjType.INT)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.INT));
            return (int) obj;
        }

        public long as_long() {
            if (type != ObjType.LONG)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.LONG));
            return (long) obj;
        }

        public float as_float() {
            if (type != ObjType.FLOAT)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.FLOAT));
            return (float) obj;
        }

        public double as_double() {
            if (type != ObjType.DOUBLE)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.DOUBLE));
            return (double) obj;
        }

        public String as_string() {
            if (!ObjType.is_type_string(type))
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is STRING");
            return (String) obj;
        }
    }
}
