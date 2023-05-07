package db;

import java.io.IOException;
import java.io.RandomAccessFile;

public class NoCache implements Cache {
    private DBFile db;

    public NoCache(DBFile db) {
        this.db = db;
    }

    @Override
    public byte[] read(int page_id, int pos, int length) throws IOException {
        RandomAccessFile ram = db.ram;
        long offset = db.get_page_offset(page_id) + pos;
        byte[] data = new byte[length];
        ram.seek(offset);
        int sz = ram.read(data);
        if (sz != length) {
            throw new IOException("incomplete read, expected " + length + " bytes, but got " + sz + " bytes");
        }
        return data;
    }

    @Override
    public void write(int page_id, int pos, byte[] data, int offset, int length) throws IOException {
        RandomAccessFile ram = db.ram;
        long file_offset = db.get_page_offset(page_id) + pos;
        ram.seek(file_offset);
        ram.write(data, offset, length);
    }

    @Override
    public void sync() throws IOException {
        db.ram.getFD().sync();
    }
}
