package jcav.filelayer.btree;

import jcav.filelayer.Bytes;
import jcav.filelayer.exception.DBRuntimeError;

import java.util.*;

/**
 * Payload models the columns in a table. A payload is either a key or a value in b-tree.
 *
 * Payload always stores data in memory.
 */
public class Payload implements Comparable<Payload> {
    private List<Integer> types = new ArrayList<>();
    private byte[] data = null;


    /**
     * make sure no other object can modify the data
     *
     * @param types, {@link ObjType}
     * @param data
     */
    public Payload(List<Integer> types, byte[] data) {
        if (types == null) throw new NullPointerException("types must not be null");
        if (data == null) throw new NullPointerException("data must not be null");
        if (data.length != get_size(types)) {
            throw new DBRuntimeError("data size mismatch, expect " + get_size(types)
                    + " bytes, got " + data.length + " bytes");
        }

        this.types = new ArrayList<>(types);
        this.data = data;
    }

    public int get_obj_count() {
        return types.size();
    }

    public int get_obj_size(int idx) {
        return ObjType.get_size(types.get(idx));
    }

    private int get_obj_offset(int idx) {
        int offset = types.size();
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

        if (data.length < offset + size)
            throw new DBRuntimeError("data too short");

        Object val = null;
        if (data[idx] == 1) {
            // null
        } else if (type == ObjType.INT) {
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

        if (data.length < offset + size)
            throw new DBRuntimeError("data too short");

        if (val.obj == null) {
            // special case for null
            byte[] val_bytes = new byte[ObjType.get_size(type)];
            System.arraycopy(val_bytes, 0, data, offset, size);
            data[idx] = 1;
        } else if (type == ObjType.INT) {
            byte[] val_bytes = Bytes.from_int(val.as_int());
            System.arraycopy(val_bytes, 0, data, offset, size);
            data[idx] = 0;
        } else if (type == ObjType.LONG) {
            byte[] val_bytes = Bytes.from_long(val.as_long());
            System.arraycopy(val_bytes, 0, data, offset, size);
            data[idx] = 0;
        } else if (type == ObjType.FLOAT) {
            byte[] val_bytes = Bytes.from_float(val.as_float());
            System.arraycopy(val_bytes, 0, data, offset, size);
            data[idx] = 0;
        } else if (type == ObjType.DOUBLE){
            byte[] val_bytes = Bytes.from_double(val.as_double());
            System.arraycopy(val_bytes, 0, data, offset, size);
            data[idx] = 0;
        } else if (ObjType.is_type_string(type)) {
            byte[] str_bytes = Bytes.from_string(val.as_string());
            System.arraycopy(str_bytes, 0, data, offset, str_bytes.length);
            if (str_bytes.length < size) {
                Arrays.fill(data, offset + str_bytes.length, offset + size, (byte) 0);
            }
            data[idx] = 0;
        } else {
            throw new DBRuntimeError("unknown type " + ObjType.to_string(type));
        }
    }

    public static boolean is_compatible(List<Integer> types_a, List<Integer> types_b) {
        boolean compatible = true;
        if (types_a.size() != types_b.size()) compatible = false;
        if (compatible) {
            for (int i = 0; i < types_a.size(); i++) {
                if (types_a.get(i) != types_b.get(i)) {
                    if (ObjType.is_type_string(types_a.get(i)) && ObjType.is_type_string(types_b.get(i))) {
                        // string type with different lens is compatible
                        continue;
                    }
                    compatible = false;
                    break;
                }
            }
        }
        return compatible;
    }

    public static int get_size(List<Integer> types) {
        int sz = types.size() + ObjType.get_size(types);
        return sz;
    }

    public static int get_size(int[] types) {
        return types.length + ObjType.get_size(types);
    }

    public boolean is_compatible(Payload o) {
        return is_compatible(types, o.types);
    }

    public List<Integer> get_types() {
        return Collections.unmodifiableList(types);
    }

    public byte[] get_bytes() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("(");
        for (int i = 0; i < types.size(); i++) {
            if (i > 0) sbuf.append(", ");
            sbuf.append(get_obj(i));
        }
        sbuf.append(")");
        return sbuf.toString();
    }

    public static Payload create(List<Integer> types, List<Object> objs) {
        if (types == null) throw new NullPointerException("types must not be null");
        if (objs == null) throw new NullPointerException("objs must not be null");
        if (types.size() != objs.size()) throw new IllegalArgumentException("types.size() != objs.size()");

        byte[] data = new byte[get_size(types)];

        Payload payload = new Payload(types, data);
        for (int i = 0; i < objs.size(); i++) {
            payload.set_obj(i, new ObjValue(types.get(i), objs.get(i)));
        }
        return payload;
    }

    public static Creator creator() {
        return new Creator();
    }

    @Override
    public int compareTo(Payload o) {
        if (o == null) throw new NullPointerException("o must not be null");
        // check compatibility

        if (!is_compatible(o)) {
            StringBuilder msg = new StringBuilder();
            msg.append("can not compare 2 incompatible payloads, ");
            msg.append("this = ").append(ObjType.to_string(types));
            msg.append(", o = ").append(ObjType.to_string(o.types));
            throw new IllegalArgumentException("can not compare 2 incompatible payloads.");
        }

        // compare
        for (int i = 0; i < types.size(); i++) {
            ObjValue v1 = get_obj(i);
            ObjValue v2 = o.get_obj(i);
            if (v1.type == ObjType.INT) {
                Integer i1 = v1.as_int();
                Integer i2 = v2.as_int();
                if (i1 == null) i1 = Integer.MIN_VALUE;
                if (i2 == null) i2 = Integer.MIN_VALUE;
                if (i1 < i2) return -1;
                if (i1 > i2) return 1;
            } else if (v1.type == ObjType.LONG) {
                Long l1 = v1.as_long();
                Long l2 = v2.as_long();
                if (l1 == null) l1 = Long.MIN_VALUE;
                if (l2 == null) l2 = Long.MIN_VALUE;
                if (l1 < l2) return -1;
                if (l1 > l2) return 1;
            } else if (v1.type == ObjType.FLOAT) {
                Float f1 = v1.as_float();
                Float f2 = v2.as_float();
                if (f1 == null) f1 = Float.MIN_VALUE;
                if (f2 == null) f2 = Float.MIN_VALUE;
                if (f1 < f2) return -1;
                if (f1 > f2) return 1;
            } else if (ObjType.is_type_string(v1.type)) {
                String s1 = v1.as_string();
                String s2 = v2.as_string();
                if (s1 == null) s1 = "";
                if (s2 == null) s2 = "";
                int cmp = s1.compareTo(s2);
                if (cmp < 0) return -1;
                if (cmp > 0) return 1;
            } else {
                throw new DBRuntimeError("unknown type " + ObjType.to_string(v1.type));
            }
        }

        return 0;
    }

    public static final class ObjValue {
        private int type;
        private Object obj;

        public ObjValue(int type, Object obj) {
            this.type = type;
            this.obj = obj;

            if (obj != null && obj.getClass() == String.class){
                if (!ObjType.is_type_string(type))
                    throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                            + ", target type is " + ObjType.to_string(ObjType.STRING(0)));
                if (((String) obj).length() > ObjType.string_len(type)) {
                    throw new DBRuntimeError("string length is too long, max length is " + ObjType.string_len(type));
                }
            }
        }

        public int get_type() {
            return type;
        }

        public Object get_obj() {
            return obj;
        }

        public Integer as_int() {
            if (type != ObjType.INT)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.INT));
            return obj == null ? null : (Integer) obj;
        }

        public Long as_long() {
            if (type != ObjType.LONG)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.LONG));
            return obj == null ? null : (Long) obj;
        }

        public Float as_float() {
            if (type != ObjType.FLOAT)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.FLOAT));
            return obj == null ? null : (Float) obj;
        }

        public Double as_double() {
            if (type != ObjType.DOUBLE)
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is " + ObjType.to_string(ObjType.DOUBLE));
            return obj == null ? null : (Double) obj;
        }

        public String as_string() {
            if (!ObjType.is_type_string(type))
                throw new DBRuntimeError("type mismatch, obj type is " + ObjType.to_string(type)
                        + ", target type is STRING");
            return obj == null ? null : (String) obj;
        }

        @Override
        public String toString() {
            if (obj == null) return null;
            if (ObjType.is_type_string(type)) {
                return "\"" + obj + "\"";
            }
            return Objects.toString(obj);
        }
    }

    public static final class Creator {
        List<Integer> val_types = new ArrayList<>();
        List<Object> val_objs = new ArrayList<>();

        public Creator val(Integer val) {
            val_types.add(ObjType.INT);
            val_objs.add(val);
            return this;
        }

        public Creator val(Long val) {
            val_types.add(ObjType.LONG);
            val_objs.add(val);
            return this;
        }

        public Creator val(Float val) {
            val_types.add(ObjType.FLOAT);
            val_objs.add(val);
            return this;
        }

        public Creator val(Double val) {
            val_types.add(ObjType.DOUBLE);
            val_objs.add(val);
            return this;
        }

        public Creator val(int len, String val) {
            val_types.add(ObjType.STRING(len));
            val_objs.add(val);
            return this;
        }

        public Payload create() {
            return Payload.create(val_types, val_objs);
        }
    }
}
