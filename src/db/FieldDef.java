package db;

import java.lang.reflect.Field;

/**
 * FieldDef is a class that defines a field in headers.
 *
 * @see Headers
 */
public final class FieldDef implements Cloneable {
    /**
     * The length of the field, in bytes.
     */
    public int len;
    /**
     * The name of the field. This is used to get the field from the Headers class. And this will not be written to the
     * file.
     * @see Headers
     */
    public String name;
    public byte[] default_value;
    /**
     * The type of the field, such as int.class, String.class, int[].class, etc.
     * Used to deserialize the field from byte array.
     */
    public Class type;

    /**
     * prefer to use other constructors. they will set the type automatically.
     *
     * @param len
     * @param name
     * @param type
     * @param default_value
     */
    public FieldDef(int len, String name, Class type, byte[] default_value) {
        this.len = len;
        this.name = name;
        this.default_value = default_value;
        this.type = type;
    }

    /**
     * A field that holds an int array.
     *
     * ATTENTION: the len is the size of bytes to store the int array, not the length of the array. That is to say, if
     * the len is 4, then the int array can only have 1 element. When deserializing, the int array will be padded
     * with 0.
     *
     * @param len
     * @param name
     * @param default_value
     */
    public FieldDef(int len, String name, int[] default_value) {
        this.len = len;
        this.name = name;
        this.default_value = Bytes.from_ints(default_value);
        this.type = int[].class;
    }

    /**
     * A field that holds a null-terminated String.
     *
     * The len is the maximum length of the string. At serialization, the string will be padded with 0 to be null
     * terminated. At deserialization, the string will be trimmed to the first null character.
     *
     * @param len
     * @param name
     * @param default_value
     */
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

    @Override
    public Object clone() {
        FieldDef obj = new FieldDef(this.len, this.name, this.type, this.default_value.clone());
        return obj;
    }
}
