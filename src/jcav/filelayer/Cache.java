package jcav.filelayer;

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

    /**
     * write some data to the cache, the cache will determine whether to write back to file or not.
     *
     * @param page_id
     * @param pos
     * @param data
     * @param offset
     * @param length
     */
    void write(int page_id, int pos, byte[] data, int offset, int length);

    /**
     * @see #write(int, int, byte[], int, int)
     * @param page_id
     * @param pos
     * @param data
     */
    default void write(int page_id, int pos, byte[] data) {
        write(page_id, pos, data, 0, data.length);
    }

    /**
     * write back all pages to file.
     *
     * @throws IOException
     */
    void sync() throws IOException;

}
