package db.btree;

import db.DBFile;
import db.FieldDef;
import db.PageType;
import db.exception.DBRuntimeError;

import java.util.*;

public class BInteriorNode extends BTreeNode {
    public static List<FieldDef> BASIC_HDR_DEFS = new ArrayList(){{
        add(new FieldDef(1, "type", PageType.BTREE_INTERIOR));
        add(new FieldDef(4, "hdr_size", 0));
        add(new FieldDef(4, "father", 0));
        add(new FieldDef(4, "cell_size", 0));
        add(new FieldDef(4, "cell_count", 0));
        add(new FieldDef(4, "free_cell", -1));
        add(new FieldDef(4, "key_count", 0));
        add(new FieldDef(4, "value_count", 0));
        //                                                                  Interior       Leaf
        add(new FieldDef(4, "total", 0));      //     total          overflow_page
        add(new FieldDef(4, "tail_child", 0)); //     tail_child     left_sibling
        add(new FieldDef(4, "reserved3", 0));  //     not used       right_sibling
        // key_types, int[]
        // value_types, int[]

        // slot_capacity, int
        // slot_count, int, meaningful slot value count in slots
        // slots, int[], fixed length specified by slot_capacity
    }};

    public BInteriorNode(int page_id, DBFile owner) {
        super(page_id, owner);
        construct_headers(BASIC_HDR_DEFS);
        if (get_page_type() != PageType.BTREE_LEAF) {
            throw new DBRuntimeError("page type mismatch, " +
                    "expect " + PageType.to_string(PageType.BTREE_LEAF)
                    + ", got " + PageType.to_string(get_page_type()));
        }
    }

    public static BInteriorNode create(int page_id, DBFile owner, int[] key_types, int[] val_types) {
        BTreeNode.create(page_id, owner, BASIC_HDR_DEFS, key_types, val_types);
        BInteriorNode node = new BInteriorNode(page_id, owner);
        return node;
    }


    ////////////////////////////////////////////////////////////
    // cell operations by slot id
    ////////////////////////////////////////////////////////////
    protected InteriorCell get_slot_cell(int slot_id) {
        if (slot_id < 0 || slot_id >= get_slot_count()) {
            throw new RuntimeException("slot_id out of range");
        }
        int cell_id = get_slot(slot_id);
        byte[] data = read_cell_data(cell_id);
        return new InteriorCell(cell_id, data, get_key_types());
    }

    protected void add_slot_cell(int slot_id, InteriorCell cell) {
        if (get_slot_count() + 1 > get_slot_capacity()) {
            throw new DBRuntimeError("no more slot available");
        }

        List<Integer> slots = get_slot_list();
        slots.add(slot_id, cell.cell_id);
        set_slots(slots);

        write_cell_data(cell.cell_id, cell.data);
    }

    protected InteriorCell remove_slot_cell(int slot_id) {
        if (slot_id < 0 || slot_id >= get_slot_count()) {
            throw new RuntimeException("slot_id out of range");
        }
        int cell_id = get_slot(slot_id);
        InteriorCell cell = get_slot_cell(slot_id);

        remove_slot(slot_id);
        release_cell(cell_id);

        return cell;
    }

    /////////////////////////////////////////////////////
    // key & child operations
    /////////////////////////////////////////////////////

    public int get_tail_child() {
        return headers.get("tail_child").to_int();
    }

    public void set_tail_child(int page_id) {
        headers.set("tail_child", page_id);
    }

    /**
     * @param slot_id, if slot_id = slot_count, set the tail child
     * @param page_id
     */
    public void set_child(int slot_id, int page_id) {
        int slot_count = get_slot_count();

        if (slot_id < 0 || slot_id > slot_count) {
            throw new RuntimeException("slot_id out of range");
        }

        if (slot_id < slot_count) {
            int cell_id = get_slot(slot_id);
            InteriorCell cell = get_slot_cell(slot_id);
            cell.set_child(page_id);
            write_cell_data(cell_id, cell.data);
        } else {
            set_tail_child(page_id);
        }
    }

    public int get_child(int slot_id) {
        int slot_count = get_slot_count();

        if (slot_id < 0 || slot_id > slot_count) {
            throw new RuntimeException("slot_id out of range");
        }

        if (slot_id < slot_count) {
            int cell_id = get_slot(slot_id);
            InteriorCell cell = get_slot_cell(slot_id);
            return cell.get_child();
        } else {
            return get_tail_child();
        }
    }

    public void set_key(int slot_id, Payload key) {
        int slot_count = get_slot_count();

        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range");
        }

        int cell_id = get_slot(slot_id);
        InteriorCell cell = get_slot_cell(slot_id);
        cell.set_key(key);
        write_cell_data(cell_id, cell.data);
    }

    public Payload get_key(int slot_id) {
        int slot_count = get_slot_count();

        if (slot_id < 0 || slot_id >= slot_count) {
            throw new IndexOutOfBoundsException("slot_id out of range");
        }

        InteriorCell cell = get_slot_cell(slot_id);
        return cell.get_key();
    }

    public List<Payload> get_keys() {
        List<Payload> keys = new ArrayList<>();
        int[] cell_ids = get_slot_array();
        for (int cell_id : cell_ids) {
            InteriorCell cell = get_slot_cell(cell_id);
            keys.add(cell.get_key());
        }
        return keys;
    }


    //////////////////////////////////////////////////
    // BTree operation
    //////////////////////////////////////////////////

    protected void insert(BPseudoInterior pseudo) {
        if (get_slot_count() + 1 > get_slot_capacity()) {
            throw new DBRuntimeError("no more slot available");
        }

        Payload key = pseudo.key;
        List<Payload> keys = get_keys();
        int idx = Collections.binarySearch(keys, key);
        if (idx >= 0) {
            throw new DBRuntimeError("key already exists");
        }

        idx = -(idx+1);
        int cell_id = allocate_cell();
        InteriorCell cell = InteriorCell.create(cell_id, get_key_types());
        add_slot_cell(idx, cell);
        set_child(idx, pseudo.left.get_page_id());
        set_child(idx+1, pseudo.right.get_page_id());
        pseudo.left.set_father(this.get_father());
        pseudo.right.set_father(this.get_father());

        int total = get_total();
        total += pseudo.left.get_total();
        total += pseudo.right.get_total();
        set_total(total);
    }

    private void update_total() {
        int total = 0;
        for (int i = 0; i < get_slot_count(); i++) {
            int child_id = get_child(i);
            BTreeNode child = new BTreeNode(child_id, owner);
            total += child.get_total();
        }
        set_total(total);
    }

    public SplitResult split() {
        int root_page = 0;
        int father_page = get_father();
        if (father_page != 0) {
            BInteriorNode father = new BInteriorNode(father_page, owner);
            if (father.get_slot_count() + 1 > father.get_slot_capacity()) {
                root_page = father.split().root_page_id;
            }
        }

        int right_page_id = owner.alloc_page();
        BInteriorNode right = BInteriorNode.create(
            right_page_id, owner, get_key_types(), get_value_types()
        );

        int mid = get_slot_count() / 2;
        List<Payload> keys = get_keys();
        Payload key = get_key(mid);

        // make right node
        for (int i = mid+1; i < get_slot_count(); i++) {
            int cell_id = get_slot(i);
            InteriorCell cell = get_slot_cell(i);
            right.add_slot_cell(i-mid-1, cell);
            right.write_cell_data(cell_id, cell.data);

            int child_id = cell.get_child();
            BTreeNode child = new BTreeNode(child_id, owner);
            child.set_father(right_page_id);
        }
        right.set_tail_child(get_tail_child());
        BTreeNode tail_child = new BTreeNode(get_tail_child(), owner);
        tail_child.set_father(right_page_id);
        right.update_total();

        // make left node
        int old_count = get_slot_count();
        int mid_child_id = get_child(mid);
        set_tail_child(mid_child_id);
        for (int i = old_count - 1;i >= mid;i--){
            remove_slot_cell(i);
        }
        set_total(get_total() - right.get_total());

        BPseudoInterior pseudo = new BPseudoInterior(
            key, this, right
        );

        if (get_father() == 0) {
            root_page = pseudo.to_interior().page_id;
        } else {
            BInteriorNode father = new BInteriorNode(get_father(), owner);
            father.insert(pseudo);
        }

        SplitResult r = new SplitResult();
        r.root_page_id = root_page;
        r.left = this;
        r.right = right;
        r.key = key;
        return r;
    }

    public int get_heir_idx(int page_id) {
        int slot_count = get_slot_count();
        for (int i = 0; i < slot_count; i++) {
            int child_id = get_child(i);
            if (child_id == page_id) {
                return i;
            }
        }
        throw new DBRuntimeError("page_id not found");
    }

    public DeleteResult delete_self() {
        if (get_slot_count() > 0) {
            throw new DBRuntimeError("can't delete non-empty node");
        }
        DeleteResult dr = new DeleteResult();

        int father_page_id = get_father();
        if (father_page_id == 0) {
            dr.root_page_id = get_tail_child();
            BTreeNode root = new BTreeNode(dr.root_page_id, owner);
            root.set_father(0);
            return dr;
        }

        boolean solved_by_borrow = false;
        BInteriorNode father = new BInteriorNode(father_page_id, owner);
        int heir_idx = father.get_heir_idx(this.page_id);
        if (heir_idx > 0) {
            // try borrow from left sibling
            int left_page_id = father.get_child(heir_idx-1);
            BTreeNode left = new BTreeNode(left_page_id, owner);
            if (left.get_slot_count() >= 2) {
                borrow_from_left(heir_idx);
                solved_by_borrow = true;
            }
        }
        if (!solved_by_borrow && heir_idx < father.get_slot_count()) {
            BInteriorNode right = new BInteriorNode(
                father.get_child(heir_idx+1), owner
            );
            if (right.get_slot_count() >= 2) {
                borrow_from_right(heir_idx);
                solved_by_borrow = true;
            }
        }
        if (solved_by_borrow) {
            return dr;
        }

        // merge
        boolean solved_by_merge = false;
        if (heir_idx > 0) {
            // try merge with left sibling
            //       (10,    80)                       (80)
            //    5    empty    100    -->       (5,10)       100
            //  A  B     C     X   X           A   B   C    X     X
            BInteriorNode left = new BInteriorNode(
                father.get_child(heir_idx-1), owner
            );
            Payload fth_key = father.get_key(heir_idx-1);
            int B = left.get_tail_child();
            int C = get_child(0);
            BTreeNode page_C = new BTreeNode(C, owner);

            // left
            InteriorCell cell = InteriorCell.create(
                left.allocate_cell(),
                left.get_key_types()
            );
            cell.set_key(fth_key);
            cell.set_child(B);
            left.add_slot_cell(left.get_slot_count(), cell);
            left.set_tail_child(C);
            page_C.set_father(left.page_id);
            left.set_total(left.get_total() + page_C.get_total());

            // father
            father.set_child(heir_idx, left.page_id);
            father.remove_slot_cell(heir_idx-1);

            solved_by_merge = true;
        }

        if (!solved_by_merge && heir_idx < father.get_slot_count()) {
            // try merge with right sibling
            //       (10,    80)                      (10)
            //    5    empty    100    -->         5       (80,100)
            //  X  X     A     B   C             X   X    A   B    C
            BInteriorNode right = new BInteriorNode(
                father.get_child(heir_idx+1), owner
            );
            Payload fth_key = father.get_key(heir_idx);
            int A = get_tail_child();
            BTreeNode page_A = new BTreeNode(A, owner);

            // right
            InteriorCell cell = InteriorCell.create(
                right.allocate_cell(),
                right.get_key_types()
            );
            cell.set_key(fth_key);
            cell.set_child(A);
            right.add_slot_cell(0, cell);
            page_A.set_father(right.page_id);
            right.set_total(right.get_total() + page_A.get_total());

            // father
            father.remove_slot_cell(heir_idx);
            solved_by_merge = true;
        }

        if (!solved_by_merge) {
            throw new DBRuntimeError("absurd structure");
        }

        owner.release_page(page_id);

        if (father.get_slot_count() > 0) {
            return dr;
        }
        dr = father.delete_self();
        return dr;
    }

    private void borrow_from_left(int heir_idx) {
        BInteriorNode fth = new BInteriorNode(get_father(), owner);
        int left_page_id = fth.get_child(heir_idx-1);
        BInteriorNode left = new BInteriorNode(left_page_id, owner);

        //           7                          5
        //   (3, 5)     empty     -->        3       7
        // A   B   C      D               A   B    C   D
        int B = left.get_child(left.get_slot_count()-1);
        int C = left.get_child(left.get_slot_count());
        Payload left_key = left.get_key(left.get_slot_count()-1);
        Payload fth_key = fth.get_key(heir_idx-1);

        // left
        left.set_tail_child(B);
        left.remove_slot_cell(left.get_slot_count()-1);
        BTreeNode page_C = new BTreeNode(C, owner);
        left.set_total(left.get_total() - page_C.get_total());

        // right
        InteriorCell cell = InteriorCell.create(allocate_cell(), get_key_types());
        cell.set_child(C);
        cell.set_key(fth_key);
        add_slot_cell(0, cell);
        set_total(get_total() + page_C.get_total());
        page_C.set_father(page_id);

        // father
        fth.set_key(heir_idx-1, left_key);
    }

    private void borrow_from_right(int heir_idx) {
        BInteriorNode fth = new BInteriorNode(get_father(), owner);
        int right_page_id = fth.get_child(heir_idx+1);
        BInteriorNode right = new BInteriorNode(right_page_id, owner);

        //           7                            10
        //   empty      (10, 15)     -->      7       15
        //     A       B   C     D         A   B     C   D
        int B = right.get_child(0);
        int C = right.get_child(1);
        Payload right_key = right.get_key(0);
        Payload fth_key = fth.get_key(heir_idx);
        BTreeNode page_B = new BTreeNode(B, owner);

        // right
        right.remove_slot_cell(0);
        right.set_total(right.get_total() - page_B.get_total());

        // left
        InteriorCell cell = InteriorCell.create(allocate_cell(), get_key_types());
        cell.set_child(get_tail_child());
        cell.set_key(fth_key);
        add_slot_cell(0, cell);
        set_tail_child(B);
        page_B.set_father(page_id);
        set_total(get_total() + page_B.get_total());

        // father
        fth.set_key(heir_idx, right_key);
    }
}
