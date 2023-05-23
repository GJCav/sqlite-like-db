package jcav.filelayer;

import jcav.filelayer.exception.DBRuntimeError;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * no-cache cache policy, directly read and write on file.
 *
 * RandomAccessFile has some wierd behavior, preferring LRUCache.
 * @see LRUCache
 */
public class NoCache implements Cache {
    private DBFile db;
    private RandomAccessFile ram;

    public NoCache(DBFile db) {
        this.db = db;
        try {
            ram = new RandomAccessFile(db.path, "rwd");
        } catch (FileNotFoundException e) {
            throw new DBRuntimeError("create cache error", e);
        }
    }

    @Override
    public byte[] read(int page_id, int pos, int length) {
        try {
            long offset = db.get_page_offset(page_id) + pos;
            byte[] data = new byte[length];
            ram.seek(offset);
            int sz = ram.read(data);
            if (sz != length) {
                throw new IOException("incomplete read, expected " + length + " bytes, but got " + sz + " bytes");
            }
            return data;
        }catch (IOException e) {
            throw new DBRuntimeError("IO read error", e);
        }
    }

    @Override
    public void write(int page_id, int pos, byte[] data, int offset, int length) {
        try {
            long file_offset = db.get_page_offset(page_id) + pos;
            ram.seek(file_offset);
            ram.write(data, offset, length);
        } catch (IOException e) {
            throw new DBRuntimeError("IO write error", e);
        }
    }

    @Override
    public void sync() throws IOException {
        ram.getFD().sync();
    }

    public void close() throws IOException {
        ram.close();
    }
}
