package jcav.filelayer;

import java.nio.charset.StandardCharsets;

/**
 * this helpful class case any type to byte array in big-endian order,
 * and cast byte array to any type.
 * most of the code is generated by Copilot.
 */
public class Bytes {
    public static byte[] from_byte(byte val) {
        return new byte[]{val};
    }

    public static byte to_byte(byte[] arr, int offset) {
        return arr[offset];
    }
    public static byte to_byte(byte[] arr) {
        return to_byte(arr, 0);
    }

    public static byte[] from_short(short val) {
        return new byte[]{
                (byte) ((val >> 8) & 0xff),
                (byte) (val & 0xff)
        };
    }

    public static short to_short(byte[] arr, int offset) {
        return (short) (((arr[offset] & 0xff) << 8) | (arr[offset + 1] & 0xff));
    }

    public static short to_short(byte[] arr) {
        return to_short(arr, 0);
    }

    public static byte[] from_int(int val) {
        return new byte[]{
                (byte) ((val >> 24) & 0xff),
                (byte) ((val >> 16) & 0xff),
                (byte) ((val >> 8) & 0xff),
                (byte) (val & 0xff)
        };
    }

    public static int to_int(byte[] arr, int offset) {
        return ((arr[offset] & 0xff) << 24)
                | ((arr[offset + 1] & 0xff) << 16)
                | ((arr[offset + 2] & 0xff) << 8) |
                (arr[offset + 3] & 0xff);
    }

    public static int to_int(byte[] arr) {
        return to_int(arr, 0);
    }

    public static byte[] from_long(long val) {
        return new byte[] {
                (byte)((val >> 56) & 0xff),
                (byte)((val >> 48) & 0xff),
                (byte)((val >> 40) & 0xff),
                (byte)((val >> 32) & 0xff),
                (byte)((val >> 24) & 0xff),
                (byte)((val >> 16) & 0xff),
                (byte)((val >> 8) & 0xff),
                (byte)(val & 0xff)
        };
    }

    public static long to_long(byte[] arr, int offset) {
        return ((arr[offset] & 0xffL) << 56)
                | ((arr[offset + 1] & 0xffL) << 48)
                | ((arr[offset + 2] & 0xffL) << 40)
                | ((arr[offset + 3] & 0xffL) << 32)
                | ((arr[offset + 4] & 0xffL) << 24)
                | ((arr[offset + 5] & 0xffL) << 16)
                | ((arr[offset + 6] & 0xffL) << 8)
                | (arr[offset + 7] & 0xffL);
    }

    public static long to_long(byte[] arr) {
        return to_long(arr, 0);
    }

    public static byte[] from_float(float val) { return from_int(Float.floatToRawIntBits(val));}

    public static Object to_float(byte[] data, int offset) {
        return Float.intBitsToFloat(to_int(data, offset));
    }

    public static Object to_float(byte[] data) {
        return to_float(data, 0);
    }

    public static byte[] from_double(double val) {
        return from_long(Double.doubleToRawLongBits(val));
    }

    public static double to_double(byte[] arr, int offset) {
        return Double.longBitsToDouble(to_long(arr, offset));
    }

    public static double to_double(byte[] arr) {
        return to_double(arr, 0);
    }

    /**
     * NOTES: the string is null-terminated.
     * @param str
     * @return
     */
    public static byte[] from_string(String str) {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        byte[] res = new byte[data.length + 1];
        System.arraycopy(data, 0, res, 0, data.length);
        return data;
    }

    /**
     * NOTES: assuming the string is null-terminated.
     * @param arr
     * @param offset
     * @param len
     * @return
     */
    public static String to_string(byte[] arr, int offset, int len) {
        int l = 0;
        while(l < len && arr[offset + l] != 0) {
            l++;
        }
        return new String(arr, offset, l, StandardCharsets.UTF_8);
    }

    /**
     * @see Bytes#to_string(byte[], int, int)
     * @param arr
     * @return
     */
    public static String to_string(byte[] arr) {
        return to_string(arr, 0, arr.length);
    }

    public static byte[] from_ints(int[] vals) {
        byte[] res = new byte[vals.length * 4];
        for(int i = 0; i < vals.length; i++) {
            System.arraycopy(from_int(vals[i]), 0, res, i * 4, 4);
        }
        return res;
    }

    public static int[] to_ints(byte[] arr, int offset, int int_len) {
        if (arr.length < offset + int_len * 4) {
            throw new IllegalArgumentException("array length is not enough");
        }
        int[] res = new int[int_len];
        for(int i = 0; i < int_len; i++) {
            res[i] = to_int(arr, offset + i * 4);
        }
        return res;
    }

    public static int[] to_ints(byte[] arr) {
        if (arr.length % 4 != 0) {
            throw new IllegalArgumentException("array length is not multiple of 4");
        }
        return to_ints(arr, 0, arr.length / 4);
    }
}
