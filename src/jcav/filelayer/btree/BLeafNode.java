package jcav.filelayer.btree;

import jcav.filelayer.*;
import jcav.filelayer.PageType;
import jcav.filelayer.exception.DBRuntimeError;
import jcav.filelayer.DBFile;
import jcav.filelayer.FieldDef;
import jcav.filelayer.OverflowPage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BLeafNode extends BTreeNode {
    public static List<FieldDef> BASIC_HDR_DEFS = new ArrayList(){{
        add(new FieldDef(1, "type", PageType.BTREE_LEAF));
        add(new FieldDef(4, "hdr_size", 0));
        add(new FieldDef(4, "father", 0));
        add(new FieldDef(4, "cell_size", 0));
        add(new FieldDef(4, "cell_count", 0));
        add(new FieldDef(4, "free_cell", -1));
        add(new FieldDef(4, "key_count", 0));
        add(new FieldDef(4, "value_count", 0));
        //                                                                      Interior       Leaf
        add(new FieldDef(4, "overflow_page", 0));  //     total          overflow_page
        add(new FieldDef(4, "left_sibling", 0));   //     tail_child     left_sibling
        add(new FieldDef(4, "right_sibling", 0));  //     not used       right_sibling
        // key_types, int[]
        // value_types, int[]

        // slot_capacity, int
        // slot_count, int, meaningful slot value count in slots
        // slots, int[], fixed length specified by slot_capacity
    }};

    private CellStorage storage;

    public BLeafNode(int page_id, DBFile owner) {
        super(page_id, owner);
        construct_headers(BASIC_HDR_DEFS);
        if (get_page_type() != PageType.BTREE_LEAF) {
            throw new DBRuntimeError("page type mismatch, " +
                    "expect " + PageType.to_string(PageType.BTREE_LEAF)
                    + ", got " + PageType.to_string(get_page_type()));
        }
        setup_overflow();
    }

    public static BLeafNode create(int page_id, DBFile owner, int[] key_types, int[] value_types) {
        BTreeNode.create(page_id, owner, BASIC_HDR_DEFS, key_types, value_types);
        BLeafNode node = new BLeafNode(page_id, owner);
        return node;
    }

    ///////////////////////////////////////////////////
    // cell operation by slot id
    ///////////////////////////////////////////////////
    public LeafCell get_slot_cell(int slot_id) {
        if (slot_id < 0 || slot_id >= get_slot_count()) {
            throw new IndexOutOfBoundsException("slot_id out of range, got " + slot_id);
        }
        int cell_id = get_slot(slot_id);
        byte[] data = read_cell_data(cell_id);
        LeafCell cell = new LeafCell(cell_id, data, get_key_types());
        return cell;
    }

    public void add_slot_cell(int slot_id, LeafCell cell) {
        if (get_slot_count() + 1 > get_slot_capacity()) {
            throw new DBRuntimeError("no more slot available");
        }

        List<Integer> slots = get_slots();
        slots.add(slot_id, cell.cell_id);
        set_slots(slots);

        write_cell_data(cell.cell_id, cell.data);
    }

    private LeafCell remove_slot_cell(int slot_id) {
        if (slot_id < 0 || slot_id >= get_slot_count()) {
            throw new RuntimeException("slot_id out of range");
        }
        int cell_id = get_slot(slot_id);
        LeafCell cell = get_slot_cell(slot_id);

        remove_slot(slot_id);
        release_cell(cell_id);
        storage.release_unit(cell.get_unit_id());
        return cell;
    }

    ////////////////////////////////////////////////////
    // key & value & overflow operations
    ////////////////////////////////////////////////////
    public int get_overflow_page() {
        return headers.get("overflow_page").to_int();
    }
    private void set_overflow_page(int page_id) {
        headers.set("overflow_page", page_id);
    }

    private void setup_overflow() {
        int overflow_page_id = get_overflow_page();
        if (overflow_page_id == 0) {
            overflow_page_id = owner.alloc_page();
            OverflowPage overflow_page = OverflowPage.create(overflow_page_id, owner);
            set_overflow_page(overflow_page_id);
            storage = CellStorage.create(overflow_page, get_value_types());
        } else {
            OverflowPage overflow_page = new OverflowPage(overflow_page_id, owner);
            storage = new CellStorage(overflow_page);

            List<Integer> storage_types = storage.get_value_type_list();
            List<Integer> node_types = get_value_type_list();
            if(!Payload.is_compatible(storage_types, node_types)) {
                throw new DBRuntimeError("incompatible value types, storage: " + storage_types + ", node: " + node_types);
            }
        }
    }



    protected void set_key(int slot_id, Payload key) {
        int slot_count = get_slot_count();
        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range, got " + slot_id);
        }
        int cell_id = get_slot(slot_id);
        LeafCell cell = get_slot_cell(slot_id);
        cell.set_key(key);
        write_cell_data(cell_id, cell.data);
    }

    public Payload get_key(int slot_id) {
        int slot_count = get_slot_count();
        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range, got " + slot_id);
        }

        LeafCell cell = get_slot_cell(slot_id);
        return cell.get_key();
    }

    public List<Payload> get_keys() {
        List<Payload> keys = new ArrayList<>();
        List<Integer> cell_ids = get_slots();
        for (int cell_id : cell_ids) {
            byte[] cell_data = read_cell_data(cell_id);
            LeafCell cell = new LeafCell(cell_id, cell_data, get_key_types());
            keys.add(cell.get_key());
        }
        return keys;
    }

    public Payload get_value(int slot_id) {
        int slot_count = get_slot_count();
        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range, got "
                    + slot_id + ", slot_count: " + slot_count);
        }

        int cell_id = get_slot(slot_id);
        LeafCell cell = get_slot_cell(slot_id);
        int unit_id = cell.get_unit_id();
        Payload value = storage.get_unit(unit_id);
        return value;
    }
    public void set_value(int slot_id, Payload value) {
        int slot_count = get_slot_count();
        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range, got "
                    + slot_id + ", slot_count: " + slot_count);
        }

        int cell_id = get_slot(slot_id);
        LeafCell cell = get_slot_cell(slot_id);
        int unit_id = cell.get_unit_id();
        storage.set_unit(unit_id, value);
    }

    private void release_self() {
        int overflow_page_id = get_overflow_page();
        if (overflow_page_id != 0) {
            owner.release_page(overflow_page_id);
        }
        owner.release_page(page_id);
    }


    //////////////////////////////////////////////////////////
    // BTree operations
    //////////////////////////////////////////////////////////
    protected void insert(Payload key, Payload val) {
        List<Payload> keys = get_keys();
        int idx = Collections.binarySearch(keys, key);
        if (idx >= 0) {
            throw new DBRuntimeError("key already exists");
        } else {
            if (keys.size() + 1 > get_slot_capacity()) {
                throw new DBRuntimeError("no more slot available");
            }

            idx = -(idx+1);
            int cell_id = allocate_cell();
            LeafCell cell = LeafCell.create(cell_id, get_key_types());
            cell.set_key(key);
            int unit_id = storage.allocate_unit();
            cell.set_unit_id(unit_id);
            storage.set_unit(unit_id, val);

            add_slot_cell(idx, cell);

            set_total(get_total()+1);
        }

        int fth = get_father();
        if (fth != 0) {
            BInteriorNode fth_page = new BInteriorNode(fth, owner);
            while(fth_page != null) {
                fth_page.set_total(fth_page.get_total()+1);
                fth = fth_page.get_father();
                if (fth == 0) fth_page = null;
                else fth_page = new BInteriorNode(fth, owner);
            }
        }
    }

    protected SplitResult split() {
        int root_page_id = 0;
        int fth = get_father();
        if(fth != 0) {
            BInteriorNode fth_page = new BInteriorNode(fth, owner);
            if (fth_page.get_slot_count() + 1 > fth_page.get_slot_capacity()) {
                SplitResult result = fth_page.split();
                root_page_id = result.root_page_id;
            }
        }

        int mid = get_slot_count() / 2;
        Payload key = get_key(mid);

        int right_page_id = owner.alloc_page();
        BLeafNode right_page = BLeafNode.create(
                right_page_id,
                owner,
                get_key_types(),
                get_value_types()
        );

        for (int i = mid + 1; i < get_slot_count(); i++) {
            LeafCell src = get_slot_cell(i);

            int dst_cell_id = right_page.allocate_cell();
            LeafCell dst = LeafCell.create(dst_cell_id, get_key_types());
            dst.set_key(src.get_key());
            dst.set_unit_id(right_page.storage.allocate_unit());

            right_page.add_slot_cell(i - mid - 1, dst);
            right_page.storage.set_unit(
                    dst.get_unit_id(),
                    storage.get_unit(src.get_unit_id())
            );
        }
        right_page.set_total(get_total() - mid - 1);

        int old_slot_count = get_slot_count();
        for(int i = old_slot_count-1;i > mid;i--) {
            remove_slot_cell(i);
        }
        set_total(mid + 1);

        // connect linked list
        BLeafNode a = this;
        BLeafNode b = right_page;
        int c_id = this.get_right_sibling();
        a.set_right_sibling(b.get_page_id());
        b.set_right_sibling(c_id);
        if (c_id != 0) {
            BLeafNode c = new BLeafNode(c_id, owner);
            c.set_left_sibling(b.get_page_id());
        }
        b.set_left_sibling(a.get_page_id());

        BPseudoInterior pseudo = new BPseudoInterior(
                key,
                this,
                right_page
        );

        // fth may change after its father split
        fth = get_father();
        if (fth == 0) {
            root_page_id = pseudo.to_interior().get_page_id();
        } else {
            BInteriorNode fth_page = new BInteriorNode(fth, owner);
            fth_page.insert(pseudo);
        }

        SplitResult r = new SplitResult();
        r.left = this;
        r.right = right_page;
        r.key = key;
        r.root_page_id = root_page_id;
        return r;
    }

    protected DeleteResult delete(SearchResult r) {
        int idx = r.idx;
        if (idx < 0) {
            throw new DBRuntimeError("del key not found");
        }
        remove_slot_cell(idx);

        BTreeNode h = this;
        while (h != null) {
            h.set_total(h.get_total() - 1);
            if (h.get_father() != 0) {
                h = new BTreeNode(h.get_father(), owner);
            } else {
                h = null;
            }
        }

        DeleteResult dr = new DeleteResult();
        dr.root_page_id = 0;

        int fth_id = get_father();
        if (fth_id == 0 || get_slot_count() >= 1) {
            return dr; // no need to change root
        }

        BInteriorNode fth = new BInteriorNode(fth_id, owner);
        int heir_idx = fth.get_heir_idx(page_id);
        boolean solve_by_borrow = false;
        if (heir_idx <= fth.get_slot_count() - 1) {
            // borrow from right
            BLeafNode right_bro = new BLeafNode(fth.get_child(heir_idx+1), owner);
            if (right_bro.get_slot_count() > 1) {
                borrow_from_right(heir_idx);
                solve_by_borrow = true;
            }
        }
        if (!solve_by_borrow && heir_idx > 0) {
            // borrow from left
            BLeafNode left_bro = new BLeafNode(fth.get_child(heir_idx-1), owner);
            if (left_bro.get_slot_count() > 1) {
                borrow_from_left(heir_idx);
                solve_by_borrow = true;
            }
        }

        if (solve_by_borrow){
            return dr; // no need to change root
        }

        // this node will be deleted

        // rearrange linked list
        int a_id = this.get_left_sibling();
        BLeafNode b = this;
        int c_id = this.get_right_sibling();
        if (a_id != 0) {
            BLeafNode a = new BLeafNode(a_id, owner);
            a.set_right_sibling(c_id);
        }
        if (c_id != 0) {
            BLeafNode c = new BLeafNode(c_id, owner);
            c.set_left_sibling(a_id);
        }

        // update father
        //        1, 3, 8
        //      A  B  C   D
        if (heir_idx == fth.get_slot_count()) {
            InteriorCell C = fth.get_slot_cell(heir_idx - 1);
            fth.remove_slot_cell(heir_idx - 1);
            fth.set_tail_child(C.get_child());
        } else {
            fth.remove_slot_cell(heir_idx);
        }

        if (fth.get_slot_count() > 0) {
            return dr; // no need to change root
        }

        release_self();

        dr = fth.delete_self();
        return dr;
    }

    private void borrow_from_left(int heir_idx) {
        BInteriorNode fth = new BInteriorNode(get_father(), owner);

        BLeafNode left_bro = new BLeafNode(fth.get_child(heir_idx-1), owner);
        LeafCell left_cell = left_bro.get_slot_cell(left_bro.get_slot_count()-1);
        Payload value = left_bro.storage.get_unit(left_cell.get_unit_id());
        left_bro.remove_slot_cell(left_bro.get_slot_count()-1);
        left_bro.set_total(left_bro.get_total()-1);

        LeafCell new_cell = LeafCell.create(allocate_cell(), get_key_types());
        new_cell.set_key(left_cell.get_key());
        int unit_id = storage.allocate_unit();
        new_cell.set_unit_id(unit_id);
        add_slot_cell(0, new_cell);
        storage.set_unit(unit_id, value);
        set_total(get_total()+1);

        fth.set_key(
                heir_idx-1,
                left_bro.get_key(left_bro.get_slot_count()-1)
        );
    }

    private void borrow_from_right(int heir_idx) {
        BInteriorNode fth = new BInteriorNode(get_father(), owner);

        BLeafNode right_bro = new BLeafNode(fth.get_child(heir_idx+1), owner);
        LeafCell right_cell = right_bro.get_slot_cell(0);
        Payload value = right_bro.storage.get_unit(right_cell.get_unit_id());
        right_bro.remove_slot_cell(0);
        right_bro.set_total(right_bro.get_total()-1);

        LeafCell new_cell = LeafCell.create(allocate_cell(), get_key_types());
        new_cell.set_key(right_cell.get_key());
        int unit_id = storage.allocate_unit();
        new_cell.set_unit_id(unit_id);
        add_slot_cell(get_slot_count(), new_cell);
        storage.set_unit(unit_id, value);
        set_total(get_total()+1);

        fth.set_key(
                heir_idx,
                right_cell.get_key()
        );
    }

    ////////////////////////////////////////////////
    // getters & setters
    ////////////////////////////////////////////////
    public int get_right_sibling() {
        return headers.get("right_sibling").to_int();
    }

    protected void set_right_sibling(int page_id) {
        headers.set("right_sibling", page_id);
    }

    public int get_left_sibling() {
        return headers.get("left_sibling").to_int();
    }

    protected void set_left_sibling(int page_id) {
        headers.set("left_sibling", page_id);
    }
}
