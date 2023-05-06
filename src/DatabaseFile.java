import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseFile implements Closeable {


    public DatabaseFile(File path) throws DBException {
        if (!Files.isReadable(path.toPath())) {
            throw new DBException("File '" + path.toString() + "' is not readable");
        }

        if (!Files.isWritable(path.toPath())) {
            throw new DBException("File '" + path.toString() + "' is not writable");
        }

        this.databasePath = path;
        try {
            this.ram = new RandomAccessFile(path, "rwd");
        } catch (IOException e) {
            throw new DBException("Failed to open file '" + path.toString() + "'", e);
        }
        this.read_header();
    }

    public void print_headers() {
        System.out.println("Headers:");
        for (FieldDef header : HEADERS) {
            System.out.println(header.name + ": " + Arrays.toString(this.header.get(header.name)));
        }
    }


    public byte[] get_header(String name) {
        return this.header.get(name);
    }


    public void set_header(String name, byte[] val) throws DBException {
        if (!is_valid_field(name, val)) {
            throw new DBException("Invalid length for field '" + name + "'");
        }
        try {
            this.ram.seek(get_field_offset(name));
            this.ram.write(val);
        } catch (IOException e) {
            throw new DBException("Failed to write header '" + name + "'", e);
        }
        this.header.put(name, val);
    }


    private void read_header() throws DBException {
        RandomAccessFile ram = this.ram;
        Map<String, byte[]> header = this.header = new HashMap<>();
        try {

            if (ram.length() < HEADER_SIZE) {
                throw new DBException("File '" + this.databasePath.toString() + "' is too small");
            }

            ram.seek(0);
            for (FieldDef def : HEADERS) {
                byte[] buf = new byte[def.len];
                int sz = ram.read(buf);
                if (sz != def.len) {
                    throw new DBException("Header incomplete: '" + def.name + "'");
                }
                header.put(def.name, buf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws IOException {
        this.ram.close();
        this.closed = true;
    }

    public static int get_field_offset(String name) {
        int offset = 0;
        for (FieldDef header : HEADERS) {
            if (header.name.equals(name)) {
                return offset;
            }
            offset += header.len;
        }
        throw new Error("Invalid database file header name: " + name);
    }

    public static int get_field_len(String name) {
        for (FieldDef header : HEADERS) {
            if (header.name.equals(name)) {
                return header.len;
            }
        }
        throw new Error("Invalid database file header name: " + name);
    }

    public static byte[] get_field_default(String name) {
        for (FieldDef header : HEADERS) {
            if (header.name.equals(name)) {
                return header.default_value;
            }
        }
        throw new Error("Invalid database file header name: " + name);
    }

    public static boolean is_valid_field(String name, byte[] arr) {
        FieldDef def = null;
        for(FieldDef d : HEADERS) {
            if (d.name.equals(name)) {
                def = d;
                break;
            }
        }
        if (def == null) {
            return false;
        }
        if (arr.length != def.len) {
            return false;
        }
        return true;
    }

    public static void create_database(File path) throws DBException {
        try (RandomAccessFile ram = new RandomAccessFile(path, "rw")){
            for (FieldDef header : HEADERS) {
                ram.write(header.default_value);
                int pad = header.len - header.default_value.length;
                while (pad-- > 0) {
                    ram.write(0);
                }
            }
            while(ram.length() < HEADER_SIZE) {
                ram.write(0);
            }
        } catch (IOException e) {
            throw new DBException("Failed to create file '" + path.toString() + "'", e);
        }
    }


    private File databasePath;
    private RandomAccessFile ram;
    private boolean closed = false;
    private Map<String, byte[]> header;

    private static class FieldDef {
        int len;
        String name;
        byte[] default_value;

        public FieldDef(int len, String name, byte[] default_value) {
            this.len = len;
            this.name = name;
            this.default_value = default_value;
        }
    }

    public static final int HEADER_SIZE = 128;

    public static final List<FieldDef> HEADERS = Arrays.asList(
            new FieldDef(32, "file_id", Bytes.fromString("SQLite-like-db")),
            new FieldDef(2, "ver", Bytes.fromShort((short) 1)),
            new FieldDef(1, "page_size", new byte[]{12}), // 4KB page size
            new FieldDef(4, "freelist_head", Bytes.fromInt(0)),
            new FieldDef(4, "freelist_count", Bytes.fromInt(0))
    );
}
