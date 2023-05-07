package db;

public final class FieldDef {
    public int len;
    public String name;
    public byte[] default_value;
    public Class type;

    public FieldDef(int len, String name, byte[] default_value) {
        this.len = len;
        this.name = name;
        this.default_value = default_value;
    }

    public FieldDef(int len, String name, String default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_string(default_value);
        this.type = String.class;
    }

    public FieldDef(int len, String name, long default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_long(default_value);
        this.type = Long.class;
    }

    public FieldDef(int len, String name, short default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_short(default_value);
        this.type = Short.class;
    }

    public FieldDef(int len, String name, byte default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_byte(default_value);
        this.type = Byte.class;
    }

    public FieldDef(int len, String name, int default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_int(default_value);
        this.type = Integer.class;
    }
}
