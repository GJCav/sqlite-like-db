package jcav.filelayer.btree;

import jcav.filelayer.DBFile;

public class BPseudoInterior {
    public Payload key;
    public BTreeNode left;
    public BTreeNode right;

    public BPseudoInterior(Payload key, BTreeNode left, BTreeNode right) {
        this.key = key;
        this.left = left;
        this.right = right;
    }

    public BInteriorNode to_interior() {
        DBFile owner = left.get_owner();
        int page_id = owner.alloc_page();
        int[] key_types = left.get_key_types();
        int[] val_types = left.get_value_types();

        BInteriorNode node = BInteriorNode.create(page_id, owner, key_types, val_types);
        node.set_key(0, key);
        node.set_child(0, left.get_page_id());
        node.set_tail_child(right.get_page_id());
        left.set_father(node.get_page_id());
        right.set_father(node.get_page_id());
        node.set_total(left.get_total() + right.get_total());

        return node;
    }
}
