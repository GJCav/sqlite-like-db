package db;

import db.exception.*;

import java.io.IOException;

public interface Cache {
    /**
     * read some data from the cache, the returned byte array is a copy of the
     * data in the cache, so it is safe to modify it, but remember to write it
     * back.
     *
     * @param page_id
     * @param pos
     * @param length
     * @return
     */
    byte[] read(int page_id, int pos, int length);
    void write(int page_id, int pos, byte[] data, int offset, int length);

    default void write(int page_id, int pos, byte[] data) {
        write(page_id, pos, data, 0, data.length);
    }

    void sync() throws IOException;

}
