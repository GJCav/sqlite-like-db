package jcav.filelayer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A quick and dirty wrapper to read and write fields in a page.
 *
 * @see FieldDef
 */
public class Headers {
    public List<FieldDef> field_defs;
    public int page_id;
    public DBFile db;

    //////////////////////////////////
    // meaningful functions
    //////////////////////////////////

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

    public static int get_total_length(List<FieldDef> defs) {
        int total = 0;
        for (FieldDef def : defs) {
            total += def.len;
        }
        return total;
    }


    ///////////////////////////////////////
    // dirty wrapper to make life easier
    //////////////////////////////////////

    public void set(String name, FieldValue value) {
        set(name, value.as_bytes());
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

    public static int get_length(List<FieldDef> defs, String name) {
        return get_field_def(defs, name).len;
    }

    public static FieldValue get_default(List<FieldDef> defs, String name) {
        FieldDef def = get_field_def(defs, name);
        return new FieldValue(def.default_value, def.type);
    }

    ///////////////////////////////////////
    // FieldValue
    //////////////////////////////////////

    /**
     * A readonly wrapper to store a field value and its type.
     */
    public static final class FieldValue {
        byte[] value;
        Class type;

        public FieldValue(byte[] value, Class type) {
            if (type == null) {
                throw new IllegalArgumentException("type cannot be null");
            }

            this.value = value;
            this.type = type;
        }

        public long as_long(){ return Bytes.to_long(this.value); }
        public int to_int(){ return Bytes.to_int(this.value); }
        public short as_short(){ return Bytes.to_short(this.value); }
        public byte as_byte(){ return Bytes.to_byte(this.value); }
        public String as_string(){ return Bytes.to_string(this.value); }
        public byte[] as_bytes(){ return this.value; }

        public Object as_object() {
            if(type == Long.class) {
                return as_long();
            } else if(type == Integer.class) {
                return to_int();
            } else if(type == Short.class) {
                return as_short();
            } else if(type == Byte.class) {
                return as_byte();
            } else if(type == String.class) {
                return as_string();
            } else if(type == byte[].class) {
                return as_bytes();
            } else if (type == int[].class) {
                return to_ints();
            }else {
                throw new RuntimeException("Unknown type: " + type);
            }
        }
        
        @Override
        public String toString() {
            if (type == int[].class) {
                return Arrays.toString(to_ints());
            }
            return Objects.toString(as_object());
        }

        public int[] to_ints() {
            return Bytes.to_ints(this.value);
        }
    }
}
