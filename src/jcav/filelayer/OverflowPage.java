package jcav.filelayer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Overflow page.
 *
 * Overflow pages are connected to form a page link, note as overflow chain. A database file can have multiple
 * overflow chain. The page contents of each page of an overflow chain is logically viewed as a continuous storage
 * space, though in the database file, they are separated in discrete location.
 *
 * Use {@link InputStream} and {@link  OutputStream} to read and write on the page.
 */
public class OverflowPage extends Page {
    public static final List<FieldDef> HEADER_DEFS = Arrays.asList(
            new FieldDef(1, "type", PageType.OVERFLOW),
            new FieldDef(4, "next", 0)
    );

    public OverflowPage(int page_id, DBFile owner) {
        super(page_id, owner);
        headers = new Headers(HEADER_DEFS, page_id, owner);
    }

    public static OverflowPage create(int page_id, DBFile owner) {
        Headers headers = new Headers(HEADER_DEFS, page_id, owner);
        headers.set_to_default();
        OverflowPage page = new OverflowPage(page_id, owner);
        return page;
    }

    public void set_next(int next) {
        headers.set("next", next);
    }

    public int get_next() {
        return headers.get("next").to_int();
    }


    public int get_available_size() {
        return owner.get_page_size(page_id) - this.get_page_header_size();
    }

    /**
     * Get an input stream from the specified position. If the position is
     * beyond the overflow chain, return null.
     * This stream will automatically skip page header.
     * @param pos
     * @return
     */
    public InputStream get_input_stream(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("pos must be >= 0");
        }

        int available_size = get_available_size();
        OverflowPage page = this;
        while (pos >= available_size) {
            pos -= available_size;
            int next_page_id = page.get_next();
            if (next_page_id == 0) {
                return null;
            }
            page = new OverflowPage(next_page_id, page.owner);
        }

        InputStream in = new InputStream(page);
        in.pos = (int) pos;
        return in;
    }

    /**
     * Get an output stream from the specified position. If the position is
     * beyond the overflow chain, allocate new pages.
     * This stream will automatically skip page header.
     * @param pos
     * @return
     */
    public OutputStream get_output_stream(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("pos must be >= 0, got " + pos);
        }

        int available_size = get_available_size();
        OverflowPage page = this;
        while (pos >= available_size) {
            pos -= available_size;
            int next_page_id = page.get_next();
            if (next_page_id == 0) {
                next_page_id = owner.alloc_page();
                OverflowPage.create(next_page_id, owner);
            }
            page = new OverflowPage(next_page_id, page.owner);
        }

        OutputStream out = new OutputStream(page);
        out.pos = (int) pos;
        return out;
    }

    public OutputStream get_output_stream() {
        return get_output_stream(0);
    }

    public InputStream get_input_stream() {
        return get_input_stream(0);
    }

    public static class OutputStream extends java.io.OutputStream {
        private  OverflowPage root_page;
        private int pos = 0;
        private OverflowPage current_page;
        private int available_size = 0;
        private int header_size = 0;

        public OutputStream(OverflowPage root_page) {
            this.root_page = root_page;
            this.current_page = root_page;
            available_size = root_page.get_available_size();
            header_size = root_page.headers.get_total_length();
        }

        @Override
        public void write(int b)  {
            if (pos == available_size) {
                if(current_page.get_next() == 0) {
                    int new_page_id = root_page.owner.alloc_page();
                    current_page.set_next(new_page_id);
                    OverflowPage.create(new_page_id, root_page.owner);
                }
                int next_page_id = current_page.get_next();
                current_page = new OverflowPage(next_page_id, root_page.owner);
                pos = 0;
            }
            current_page.write(header_size + pos, new byte[] {(byte) b});
            pos++;
        }

        @Override
        public void write(byte[] b) {
            try {
                super.write(b);
            } catch (IOException e) {
                throw new RuntimeException("no sense to happen", e);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            try {
                super.write(b, off, len);
            } catch (IOException e) {
                throw new RuntimeException("no sense to happen", e);
            }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    public static class InputStream extends java.io.InputStream {
        private OverflowPage root_page;
        private int pos = 0;
        private OverflowPage current_page;
        private int available_size = 0;
        private int header_size = 0;

        public InputStream(OverflowPage root_page)  {
            this.root_page = root_page;
            this.current_page = root_page;
            available_size = root_page.owner.get_page_size(root_page.page_id)
                    - root_page.headers.get_total_length();
            header_size = root_page.headers.get_total_length();
        }

        @Override
        public int read() {
            if (pos == available_size) {
                int next_page_id = current_page.get_next();
                if (next_page_id == 0) {
                    return -1;
                }
                current_page = new OverflowPage(next_page_id, root_page.owner);
                pos = 0;
            }
            byte b = current_page.read(header_size + pos, 1)[0];
            int v = b & 0xff; // 大坑，不能直接 (int)b，否则会返回负数，导致 EOF 判断失效
            pos++;
            return v;
        }

        @Override
        public void reset() {
            current_page = root_page;
            pos = 0;
        }

        @Override
        public int read(byte[] b) {
            try {
                return super.read(b);
            } catch (IOException e) {
                throw new RuntimeException("no sense to happen", e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            try {
                return super.read(b, off, len);
            } catch (IOException e) {
                throw new RuntimeException("no sense to happen", e);
            }
        }

        @Override
        public long skip(long n) {
            try {
                return super.skip(n);
            } catch (IOException e) {
                throw new RuntimeException("no sense to happen", e);
            }
        }

        @Override
        public int available()  {
            throw new RuntimeException("not supported");
        }

        @Override
        public void close() {}
    }
}
