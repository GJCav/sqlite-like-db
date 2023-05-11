package db;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Headers {
    public List<FieldDef> field_defs;
    public int page_id;
    public DBFile db;

    public Headers(List<FieldDef> field_defs, int page_id, DBFile db) {
        this.field_defs = field_defs;
        this.page_id = page_id;
        this.db = db;
    }

    public FieldValue get(String name) {
        int offset = get_offset(this.field_defs, name);
        FieldDef def = get_field_def(this.field_defs, name);
        int len = def.len;
        byte[] data = this.db.read(this.page_id, offset, len);
        return new FieldValue(data, def.type);
    }

    public void set(String name, byte[] value) {
        int offset = get_offset(this.field_defs, name);
        int len = get_length(this.field_defs, name);

        if (value.length <= len) {
            byte[] new_value = new byte[len];
            System.arraycopy(value, 0, new_value, 0, value.length);
            value = new_value;
        } else {
            throw new IllegalArgumentException("value too long");
        }

        this.db.write(this.page_id, offset, value, 0, len);
    }

    public void set(String name, FieldValue value) {
        set(name, value.to_bytes());
    }

    public void set(String name, byte val) {
        set(name, new byte[]{val});
    }
    public void set(String name, short val) {
        set(name, Bytes.from_short(val));
    }
    public void set(String name, int val) {
        set(name, Bytes.from_int(val));
    }
    public void set(String name, long val) {
        set(name, Bytes.from_long(val));
    }
    public void set(String name, double val) {
        set(name, Bytes.from_double(val));
    }
    public void set(String name, String val) {
        set(name, Bytes.from_string(val));
    }

    public void set_to_default() {
        for (FieldDef def : this.field_defs) {
            this.set(def.name, def.default_value);
        }
    }

    public int get_offset(String name) {
        return get_offset(this.field_defs, name);
    }

    public int get_length(String name) {
        return get_length(this.field_defs, name);
    }

    public int get_total_length() {
        return get_total_length(this.field_defs);
    }

    public FieldValue get_default(String name) {
        return get_default(this.field_defs, name);
    }

    public static FieldDef get_field_def(List<FieldDef> defs, String name) {
        for (FieldDef def : defs) {
            if (def.name.equals(name)) {
                return def;
            }
        }
        throw new IllegalArgumentException("No such field: " + name);
    }

    public static int get_offset(List<FieldDef> defs, String name) {
        int offset = 0;
        for (FieldDef def : defs) {
            if (def.name.equals(name)) {
                return offset;
            }
            offset += def.len;
        }
        throw new IllegalArgumentException("No such field: " + name);
    }

    public static int get_length(List<FieldDef> defs, String name) {
        return get_field_def(defs, name).len;
    }

    public static int get_total_length(List<FieldDef> defs) {
        int total = 0;
        for (FieldDef def : defs) {
            total += def.len;
        }
        return total;
    }

    public static FieldValue get_default(List<FieldDef> defs, String name) {
        FieldDef def = get_field_def(defs, name);
        return new FieldValue(def.default_value, def.type);
    }


    public static final class FieldValue {
        byte[] value;
        Class type;

        public FieldValue(byte[] value, Class type) {
            this.value = value;
            this.type = type;
        }

        public long to_long(){ return Bytes.to_long(this.value); }
        public int to_int(){ return Bytes.to_int(this.value); }
        public short to_short(){ return Bytes.to_short(this.value); }
        public byte to_byte(){ return Bytes.to_byte(this.value); }
        public String to_string(){ return Bytes.to_string(this.value); }
        public byte[] to_bytes(){ return this.value; }

        public Object to_object() {
            if(type == Long.class) {
                return to_long();
            } else if(type == Integer.class) {
                return to_int();
            } else if(type == Short.class) {
                return to_short();
            } else if(type == Byte.class) {
                return to_byte();
            } else if(type == String.class) {
                return to_string();
            } else if(type == byte[].class) {
                return to_bytes();
            } else {
                throw new RuntimeException("Unknown type: " + type);
            }
        }

        @Override
        public String toString() {
            return Objects.toString(to_object());
        }
    }
}
