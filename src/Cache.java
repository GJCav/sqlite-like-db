import java.util.HashMap;
import java.util.Map;

public class Cache {

    public Cache(int max_size) {
        this.blocks = new HashMap<>();
        this.active_blocks = new BlockChain();
        this.free_blocks = new BlockChain();
        this.max_size = max_size;
    }

    public void put(int block_id, byte[] data) {
        if (blocks.containsKey(block_id)) {
            Block block = this.blocks.get(block_id);
            this.active_blocks.remove(block);
            this.active_blocks.add_front(block);
        } else if (free_blocks.front() != free_blocks.end()) {
            Block block = this.free_blocks.front();
            this.free_blocks.remove(block);
            block.block_id = block_id;
            block.data = data;
            this.blocks.put(block_id, block);
            this.active_blocks.add_front(block);
        } else if (active_blocks.size < max_size) {
            Block block = new Block();
            block.block_id = block_id;
            block.data = data;
            this.blocks.put(block_id, block);
            this.active_blocks.add_front(block);
        } else {
            Block block = this.active_blocks.back();
            if (block == null) throw new RuntimeException("unable to cache block");
            this.active_blocks.remove(block);
            this.blocks.remove(block.block_id);
            block.block_id = block_id;
            block.data = data;
            this.blocks.put(block_id, block);
            this.active_blocks.add_front(block);
        }
    }

    public byte[] get(int block_id) {
        if (!blocks.containsKey(block_id)) return null;
        Block block = this.blocks.get(block_id);
        this.active_blocks.remove(block);
        this.active_blocks.add_front(block);
        return block.data;
    }

    public void mark_dirty(int block_id) {
        if (!blocks.containsKey(block_id)) return;
        Block block = this.blocks.get(block_id);
        this.blocks.remove(block_id);
        this.active_blocks.remove(block);
        this.free_blocks.add_front(block);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Active Blocks: ");
        Block block = this.active_blocks.front();
        while (block != this.active_blocks.tail) {
            sb.append(block.block_id);
            sb.append(", ");
            block = block.next;
        }
        sb.append("\nFree Blocks: ");
        block = this.free_blocks.front();
        while (block != this.free_blocks.tail) {
            sb.append(block.block_id);
            sb.append(", ");
            block = block.next;
        }
        return sb.toString();
    }

    private Map<Integer, Block> blocks;
    private BlockChain active_blocks;
    private BlockChain free_blocks;
    private int max_size;

    public static class Block {
        Block prev;
        Block next;

        int block_id;
        byte[] data;
    }

    public static class BlockChain {
        private Block head;
        private Block tail;
        private int size;

        public BlockChain() {
            this.head = new Block();
            this.tail = new Block();
            this.head.next = this.tail;
            this.tail.prev = this.head;
        }


        public Block front() {
            return head.next;
        }

        public Block back() {
            return tail.prev;
        }

        public Block end() {
            return tail;
        }

        public void add_front(Block block) {
            add(head, block);
        }

        public void remove_back() {
            if (tail.prev == head) return;
            remove(tail.prev);
        }

        public void add(Block pos, Block block) {
            if (pos == tail) throw new RuntimeException("Cannot add after tail");

            block.prev = pos;
            block.next = pos.next;
            pos.next.prev = block;
            pos.next = block;
            this.size++;
        }

        public void remove(Block pos) {
            if (pos == head || pos == tail) throw new RuntimeException("Cannot remove head or tail");

            pos.prev.next = pos.next;
            pos.next.prev = pos.prev;
            this.size--;
        }
    }
}
