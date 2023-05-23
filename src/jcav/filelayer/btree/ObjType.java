package jcav.filelayer.btree;

import java.util.HashMap;
import java.util.List;

public final class ObjType {
    public static final int INT = 1;
    public static final int LONG = 2;
    public static final int FLOAT = 3;
    public static final int DOUBLE = 4;

    /**
     * used to program
     * @param type
     * @return
     */
    public static String type_string(int type) {
        if(type < 0) throw new IllegalArgumentException("Invalid type: " + type);
        if (type < 16) {
            if (type == INT) return "INT";
            if (type == LONG) return "LONG";
            if (type == FLOAT) return "FLOAT";
            if (type == DOUBLE) return "DOUBLE";
        } else {
            return "STRING";
        }
        return "UNKNOWN(" + type + ")";
    }

    /**
     * used to print type info
     * @param type
     * @return
     */
    public static String to_string(int type) {
        String ts = type_string(type);
        if (ts.equals("STRING")) {
            return ts + "(" + (type - 16) + ")";
        } else {
            return ts;
        }
    }

    public static String to_string(List<Integer> types) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(int i = 0; i < types.size(); i++){
            sb.append(to_string(types.get(i)));
            if(i != types.size() - 1){
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static int get_size(int type) {
        if (type == INT) return 4;
        if (type == LONG) return 8;
        if (type == FLOAT) return 4;
        if (type == DOUBLE) return 8;
        if (type > 16) return type - 16;
        throw new IllegalArgumentException("Invalid type: " + type);
    }

    public static int get_size(List<Integer> types) {
        return types.stream().mapToInt(ObjType::get_size).sum();
    }

    public static int get_size(int[] types){
        int size = 0;
        for(int type : types){
            size += get_size(type);
        }
        return size;
    }

    public static boolean is_type_string(int type) {
        return type > 16;
    }

    public static int STRING(int len) {
        if (len < 0) throw new IllegalArgumentException("len must be greater than 0");
        return 16 + len;
    }

    public static HashMap<Class, Integer> class_to_type = new HashMap(){{
        put(Integer.class, ObjType.INT);
        put(Long.class, ObjType.LONG);
        put(Float.class, ObjType.FLOAT);
        put(Double.class, ObjType.DOUBLE);
        put(String.class, ObjType.STRING(255));
    }};

    public static int string_len(int type) {
        if (!is_type_string(type)) throw new IllegalArgumentException("type must be string");
        return type - 16;
    }
}
