package db;

import db.exception.DBRuntimeError;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Least Recently Used Cache for DBFile
 *
 * @see Cache
 */
public class LRUCache implements Cache {
    private DBFile db;
    private int max_cache_size = 100;
    private Map<Integer, Block> blocks = new HashMap<>();
    private BlockChain used_blocks = new BlockChain();
    private BlockChain free_blocks = new BlockChain();

    public LRUCache(DBFile db, int max_cache_size) {
        if (db == null) throw new IllegalArgumentException("db must not be null");
        if (max_cache_size <= 0)
            throw new IllegalArgumentException("max_cache_size must be positive");

        this.db = db;
        this.max_cache_size = max_cache_size;
    }

    private void release_block(Block block) {
        blocks.remove(block.page_id);
        if (block.updated) write_back(block);
        block.data = null;
        block.page_id = -1;
        free_blocks.add_back(block);
    }

    private void write_back(Block block) {
//        System.out.println("[LRUCache] write back page " + block.page_id);

        long file_offset = db.get_page_offset(block.page_id);
        try {
            db.ram.seek(file_offset);
            db.ram.write(block.data);
            block.updated = false;
        } catch (IOException e) {
            throw new RuntimeException("LRU write back error", e);
        }
    }

    private Block cache_data(int page_id, byte[] data) {
        if (free_blocks.size == 0 && blocks.size() == max_cache_size)
            release_block(used_blocks.back());

        if(free_blocks.size > 0){
            Block block = free_blocks.back();
            free_blocks.remove(block);
            block.page_id = page_id;
            block.data = data;
            blocks.put(page_id, block);
            used_blocks.add_front(block);
            return block;
        } else {
            Block block = new Block();
            block.page_id = page_id;
            block.data = data;
            blocks.put(page_id, block);
            used_blocks.add_front(block);
            return block;
        }
    }

    private byte[] read_from_file(int page_id) {
        long file_offset = db.get_page_offset(page_id);
        int page_size = db.get_page_size(page_id);
        byte[] data = new byte[page_size];

        try {
            db.ram.seek(file_offset);
            int sz = db.ram.read(data);
            if (sz != page_size)
                throw new RuntimeException("incomplete page read, page_id = " + page_id
                        + ", page_size = " + page_size + ", read_size = " + sz);
            return data;
        } catch (IOException e) {
            throw new DBRuntimeError("IO read error", e);
        }
    }

    private Block get_block(int page_id) {
        if (page_id < 0) throw new IllegalArgumentException("page_id must be positive, page_id="+page_id);
        if (blocks.containsKey(page_id)) {
//            System.out.println("[LRUCache] hit cache page " + page_id);

            Block block = blocks.get(page_id);
            used_blocks.remove(block);
            used_blocks.add_front(block);
            return block;
        } else {
            byte[] data = read_from_file(page_id);
            Block block = cache_data(page_id, data);
            return block;
        }
    }

    @Override
    public byte[] read(int page_id, int pos, int length) {
//        System.out.println("[LRUCache] read page " + page_id + ", pos = " + pos + ", length = " + length);

        Block block = get_block(page_id);
        byte[] data = new byte[length];
        System.arraycopy(block.data, pos, data, 0, length);
        return data;
    }

    @Override
    public void write(int page_id, int pos, byte[] data, int offset, int length) {
//        System.out.println("[LRUCache] write page " + page_id + ", pos = " + pos + ", length = " + length);

        Block block = get_block(page_id);
        if (block.data.length < pos + length)
            throw new IllegalArgumentException("write out of block bound");
        System.arraycopy(data, offset, block.data, pos, length);
        block.updated = true;
    }

    @Override
    public void sync() throws IOException {
        for (Block block : blocks.values()) {
            if (block.updated) write_back(block);
        }
        db.ram.getFD().sync();
    }

    public static final class Block {
        Block next;
        Block prev;

        int page_id;
        byte[] data;
        boolean updated = false;
    }

    public static final class BlockChain {
        Block head;
        Block tail;
        int size = 0;

        public BlockChain() {
            head = new Block();
            tail = new Block();
            head.next = tail;
            tail.prev = head;
        }

        public Block front() {
            return head.next;
        }

        public Block back() {
            return tail.prev;
        }


        public void add_back(Block block) {
            block.next = tail;
            block.prev = tail.prev;
            tail.prev.next = block;
            tail.prev = block;
            size++;
        }

        public void add_front(Block block) {
            block.next = head.next;
            block.prev = head;
            head.next.prev = block;
            head.next = block;
            size++;
        }

        public void remove(Block block) {
            if (block == tail || block == head)
                throw new IllegalArgumentException("cannot remove head or tail");
            block.prev.next = block.next;
            block.next.prev = block.prev;
            size--;
        }

        public void remove_back() {
            if (tail.prev == head) return;
            remove(tail.prev);
        }

        public void remove_front() {
            if (head.next == tail) return;
            remove(head.next);
        }
    }
}
