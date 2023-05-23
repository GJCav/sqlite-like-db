package jcav.test;

import java.io.*;
import java.util.*;

public class BPlusTree<K extends Comparable<K>, V> {
    public static int MAX_DEG = 3;
    public static final int INTERIOR = 1;
    public static final int LEAF = 2;

    public static final class SearchResult<K extends Comparable<K>, V> {
        public List<Node<K, V>> path = new ArrayList<>();
        public List<Integer> idxs = new ArrayList<>();

        /**
         * same to Collections.binarySearch
         *  - idx >= 0: found
         *  - idx < 0: not found, -(idx + 1) is the first element that is greater than key
         */
        int idx;

        public boolean found() { return idx >= 0; }
        public int insert_point() { return idx < 0 ? -(idx+1) : idx; }
    }

    public static final class SplitResult<K extends Comparable<K>, V> {
        public Node<K, V> left;
        public Node<K, V> right;
        public K key;
        public Node<K, V> root;
    }

    public static final class DeleteResult<K extends Comparable<K>, V> {
        public Node<K, V> root;
    }

    public static abstract class Node<K extends Comparable<K>, V> {

        public Node<K, V> father;

        /**
         * 使用顺序遍历，只会在同一个 page 中遍历
         * 但维护 heir_idx 需要遍历某个 interior node 插入位置之后的所有 children 的 page
         * 代价更大，所以不维护 heir_idx
         */
//        public int heir_idx = 0;

        public List<K> keys = new ArrayList<>();
        int total = 0;

        public int get_type() { throw new RuntimeException(); }

        public SearchResult<K, V> find(K key) {
            SearchResult<K, V> r = new SearchResult<>();

            Node<K, V> cur = this;
            while(cur != null) {
                r.path.add(cur);
                if (cur.get_type() == INTERIOR) {
                    Interior<K, V> interior = (Interior<K, V>) cur;
                    int idx = Collections.binarySearch(cur.keys, key);
                    if (idx < 0) {
                        idx = -(idx+1);
                    }
                    cur = interior.children.get(idx);
                    r.idxs.add(idx);
                } else if (cur.get_type() == LEAF) {
                    int idx = Collections.binarySearch(cur.keys, key);
                    r.idx = idx;
                    r.idxs.add(idx);
                    cur = null;
                } else {
                    throw new RuntimeException("Unknown node type");
                }
            }

            return r;
        }

        public abstract SplitResult<K, V> split();

    }

    /**
     * used in split. only contains one key, and two children
     * both children. all data is stored in the memory.
     *
     * @param <K>
     * @param <V>
     */
    public static class PseudoInterior<K extends Comparable<K>, V> {
        public K key;
        public Node<K, V> left;
        public Node<K, V> right;


        public PseudoInterior(Node<K, V> left, K key, Node<K, V> right) {
            this.left = left;
            this.key = key;
            this.right = right;
        }

        public Interior<K, V> to_interior() {
            Interior<K, V> interior = new Interior<>();
            interior.keys.add(key);
            interior.children.add(left);
            interior.children.add(right);
            left.father = interior;
            right.father = interior;
            interior.total = interior.children.stream().mapToInt(c -> c.total).sum();
            return interior;
        }
    }

    public static class Interior<K extends Comparable<K>, V> extends Node<K, V> {
        public List<Node<K, V>> children = new ArrayList<>();

        @Override
        public int get_type() { return INTERIOR; }

        private void insert(PseudoInterior<K, V> pseudo) {
            if (keys.size() + 1 > MAX_DEG) {
                throw new RuntimeException("too many keys");
            }

            K key = pseudo.key;
            int idx = Collections.binarySearch(keys, key);
            if (idx >= 0) {
                throw new RuntimeException("key already exists");
            } else {
                idx = -(idx+1);
                keys.add(idx, key);
                children.add(idx, pseudo.left);
                children.set(idx+1, pseudo.right);
                pseudo.left.father = this;
                pseudo.right.father = this;
                this.total = this.children.stream().mapToInt(c -> c.total).sum();
            }
        }

        /**
         *
         * @return new root node if root has been split, otherwise null
         */
        @Override
        public SplitResult<K, V> split() {
            /**
             * Splitting this node will cause its father to insert a new key,
             * so check its father first. And this should be done before splitting
             * current node to guarantee that the father can get the correct
             * total count during splitting.
             */
            Node<K, V> root = null;
            if(father != null && father.keys.size() + 1 > MAX_DEG) {
                root = father.split().root;
            }

            Interior<K, V> right = new Interior<>();

            int mid = keys.size() / 2;
            K key = keys.get(mid);
            right.keys.addAll(keys.subList(mid+1, keys.size()));
            right.children.addAll(children.subList(mid+1, children.size()));
            right.children.forEach(c -> c.father = right);
            right.total = right.children.stream().mapToInt(c -> c.total).sum();

            this.keys.subList(mid, keys.size()).clear();
            this.children.subList(mid+1, children.size()).clear();
            this.total = this.children.stream().mapToInt(c -> c.total).sum();

            PseudoInterior<K, V> pseudo = new PseudoInterior<>(this, key, right);

            if (father == null) {
                root = pseudo.to_interior();
            } else {
                ((Interior<K, V>) father).insert(pseudo);
            }

            SplitResult<K, V> r = new SplitResult<>();
            r.left = this;
            r.right = right;
            r.key = key;
            r.root = root;
            return r;
        }

        public DeleteResult<K,V> delete_self() {
            if (keys.size() > 0) {
                throw new RuntimeException("can't delete non-empty interior node");
            }
            DeleteResult<K, V> dr = new DeleteResult<>();
            dr.root = null;

            if (father == null) {
                dr.root = children.get(0);
                dr.root.father = null;
                // TODO: release space of this node
                return dr;
            }

            boolean solved_by_borrow = false;
            Interior<K, V> fth = (Interior<K, V>) father;
            int heir_idx = fth.children.indexOf(this);
            if (heir_idx > 0) {
                // try borrow from left sibling
                Interior<K, V> left = (Interior<K, V>) fth.children.get(heir_idx-1);
                if (left.keys.size() >= 2) {
                    borrow_from_left(heir_idx);
                    solved_by_borrow = true;
                }
            }
            if (!solved_by_borrow && heir_idx < fth.children.size()-1) {
                // try borrow from right sibling
                Interior<K, V> right = (Interior<K, V>) fth.children.get(heir_idx+1);
                if (right.keys.size() >= 2) {
                    borrow_from_right(heir_idx);
                    solved_by_borrow = true;
                }
            }
            if (solved_by_borrow){
                return dr; // no root change
            }

            boolean solved_by_merge = false;
            if (heir_idx > 0) {
                // try merge with left sibling
                Interior<K, V> left = (Interior<K, V>) fth.children.get(heir_idx-1);
                fth.children.set(heir_idx, left);
                fth.children.remove(heir_idx-1);
                K key = fth.keys.remove(heir_idx-1);

                left.keys.add(key);
                Node<K, V> child = this.children.get(0);
                left.children.add(child);
                child.father = left;
                left.total += child.total;
                solved_by_merge = true;
            }

            if (!solved_by_merge && heir_idx < fth.children.size() - 1) {
                // try merge with right sibling
                Interior<K, V> right = (Interior<K, V>) fth.children.get(heir_idx+1);
                fth.children.remove(heir_idx);
                K key = fth.keys.remove(heir_idx);

                right.keys.add(0, key);
                Node<K, V> child = this.children.get(0);
                right.children.add(0, child);
                child.father = right;
                right.total += child.total;
                solved_by_merge = true;
            }

            if (!solved_by_merge) {
                throw new RuntimeException("absurd structure");
            }

            // TODO: release space of this node

            if (fth.keys.size() > 0) {
                return dr;
            }

            dr = fth.delete_self();
            return dr;
        }

        private void borrow_from_left(int heir_idx) {
            Interior<K, V> fth = (Interior<K, V>)father;
            Interior<K, V> left = (Interior<K, V>)fth.children.get(heir_idx-1);

            K key = left.keys.remove(left.keys.size()-1);
            Node<K, V> child = left.children.remove(left.children.size()-1);
            left.total -= child.total;

            this.keys.add(fth.keys.get(heir_idx-1));
            this.children.add(0, child);
            this.total += child.total;
            child.father = this;

            fth.keys.set(heir_idx-1, key);
        }

        private void borrow_from_right(int heir_idx) {
            Interior<K, V> fth = (Interior<K, V>)father;
            Interior<K, V> right = (Interior<K, V>)fth.children.get(heir_idx+1);

            K key = right.keys.remove(0);
            Node<K, V> child = right.children.remove(0);
            right.total -= child.total;

            this.keys.add(fth.keys.get(heir_idx));
            this.children.add(child);
            this.total += child.total;
            child.father = this;

            fth.keys.set(heir_idx, key);
        }
    }

    public static final class Leaf<K extends Comparable<K>, V> extends Node<K, V> {
        public Leaf<K, V> left;
        public Leaf<K, V> right;

        public List<V> values = new ArrayList<>();
        public List<Integer> duplicate = new ArrayList<>();

        @Override
        public int get_type() {
            return LEAF;
        }

        /**
         *
         * @param key
         * @param val
         * @return new root node
         */
        private void insert(K key, V val) {
            int idx = Collections.binarySearch(keys, key);
            if (idx >= 0) {
                duplicate.set(idx, duplicate.get(idx) + 1);
                total++;
            } else {
                if (keys.size() + 1 > MAX_DEG) {
                    throw new RuntimeException("too many keys");
                }

                idx = -(idx+1);
                keys.add(idx, key);
                values.add(idx, val);
                duplicate.add(idx, 1);
                total++;
            }

            if(father != null) {
                Interior<K, V> h = (Interior<K, V>) father;
                while(h != null) {
                    h.total++;
                    h = (Interior<K, V>) h.father;
                }
            }
        }

        @Override
        public SplitResult<K, V> split() {
            /**
             * Same to Interior.split()
             */
            Node<K, V> root = null;
            if(father != null && father.keys.size() + 1 > MAX_DEG) {
                root = father.split().root;
            }

            Leaf<K, V> right = new Leaf<>();

            int mid = keys.size() / 2;
            K key = keys.get(mid);

            // 叶子节点分裂，需要在右侧保留选定的 key
            right.keys.addAll(keys.subList(mid+1, keys.size()));
            right.values.addAll(values.subList(mid+1, values.size()));
            right.duplicate.addAll(duplicate.subList(mid+1, duplicate.size()));
            right.total = right.duplicate.stream().mapToInt(i -> i).sum();

            this.keys.subList(mid+1, keys.size()).clear();
            this.values.subList(mid+1, values.size()).clear();
            this.duplicate.subList(mid+1, duplicate.size()).clear();
            this.total = this.duplicate.stream().mapToInt(i -> i).sum();

            // connect linked list
            Leaf<K, V> a = this;
            Leaf<K, V> b = right;
            Leaf<K, V> c = this.right;
            a.right = b;
            b.right = c;
            if(c != null) c.left = b;
            b.left = a;

            PseudoInterior<K, V> pseudo = new PseudoInterior<>(this, key, right);

            if (father == null) {
                root = pseudo.to_interior();
            } else {
                ((Interior<K, V>) father).insert(pseudo);
            }

            SplitResult<K, V> r = new SplitResult<>();
            r.left = this;
            r.right = right;
            r.key = key;
            r.root = root;
            return r;
        }

        public DeleteResult<K, V> delete(SearchResult<K, V> r) {
            int idx = r.idx;
            if(idx < 0) throw new RuntimeException("del key not found");

            duplicate.set(idx, duplicate.get(idx) - 1);

            if(duplicate.get(idx) == 0) {
                keys.remove(idx);
                values.remove(idx);
                duplicate.remove(idx);
            }

            Node<K, V> h = this;
            while(h != null) {
                h.total--;
                h = h.father;
            }

            DeleteResult<K, V> dr = new DeleteResult<>();
            dr.root = null;

            if(father == null || keys.size() >= 1) {
                // no need to adjust
                return dr;
            }


            Interior<K, V> fth = (Interior<K, V>) father;
            int heir_idx = fth.children.indexOf(this);
            boolean solve_by_borrow = false;
            if (heir_idx < fth.children.size() - 1) {
                // try to borrow from right sibling
                Leaf<K, V> right_brother = (Leaf<K, V>) fth.children.get(heir_idx + 1);
                if (right_brother.keys.size() > 1) {
                    borrow_from_right(heir_idx);
                    solve_by_borrow = true;
                }
            }
            if (!solve_by_borrow && heir_idx > 0) {
                // try to borrow from left sibling
                Leaf<K, V> left_brother = (Leaf<K, V>) fth.children.get(heir_idx - 1);
                if (left_brother.keys.size() > 1) {
                    borrow_from_left(heir_idx);
                    solve_by_borrow = true;
                }
            }

            if (solve_by_borrow) {
                return dr; // no root change
            }

            // this node will be released
            // rearrange the linked list
            Leaf<K, V> a = this.left;
            Leaf<K, V> b = this;
            Leaf<K, V> c = this.right;
            if(a != null) a.right = c;
            if(c != null) c.left = a;

            // merge siblings and release this node
            fth.children.remove(heir_idx);
            if (heir_idx < fth.keys.size()) {
                fth.keys.remove(heir_idx);
            } else {
                fth.keys.remove(heir_idx - 1);
            }

            // TODO: release space used by this node

            if (fth.keys.size() > 0) {
                return dr; // no root change
            }

            return fth.delete_self();
        }

        private void borrow_from_left(int heir_idx) {
            Interior<K, V> fth = (Interior<K, V>) father;
            Leaf<K, V> bro = (Leaf<K, V>) fth.children.get(heir_idx - 1);
            K key = bro.keys.remove(bro.keys.size() - 1);
            V val = bro.values.remove(bro.values.size() - 1);
            int dup = bro.duplicate.remove(bro.duplicate.size() - 1);
            bro.total -= dup;

            keys.add(key);
            values.add(val);
            duplicate.add(dup);
            total += dup;

            fth.keys.set(heir_idx - 1, bro.keys.get(bro.keys.size() - 1));
        }

        private void borrow_from_right(int heir_idx) {
            Interior<K, V> fth = (Interior<K, V>) father;
            Leaf<K, V> bro = (Leaf<K, V>) fth.children.get(heir_idx + 1);
            K key = bro.keys.remove(0);
            V val = bro.values.remove(0);
            int dup = bro.duplicate.remove(0);
            bro.total -= dup;

            keys.add(key);
            values.add(val);
            duplicate.add(dup);
            total += dup;

            fth.keys.set(heir_idx, key);
        }
    }

    private Node<K, V> root;

    public BPlusTree() {
        root = new Leaf();
    }

    public void insert(K key, V val) {
        SearchResult<K, V> r = root.find(key);
        Node<K, V> node = r.path.get(r.path.size()-1);
        if(node.get_type() != LEAF) {
            throw new RuntimeException("find error");
        }
        Leaf<K, V> leaf = (Leaf<K, V>)node;
        if (r.found()) {
            // good, no split possibility
        } else if (leaf.keys.size() + 1 > MAX_DEG) {
            SplitResult<K, V> sr = leaf.split();
            root = sr.root == null ? root : sr.root;
            int cmp = key.compareTo(sr.key);
            if (cmp > 0) {
                leaf = (Leaf<K, V>)sr.right;
            } else if (cmp < 0){
                leaf = (Leaf<K, V>)sr.left;
            } else {
                throw new RuntimeException("error cmp");
            }
        }
        leaf.insert(key, val);
    }

    public SearchResult<K, V> find(K key) {
        return root.find(key);
    }

    public V prior(K key) {
        SearchResult<K, V> r = root.find(key);
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);
        int idx = r.idx;
        if (idx < 0) idx = -(idx+1);

        do {
            idx--;
            if (idx == -1) {
                if (leaf.left == null) {
                    System.out.println("find " + key);
                    throw new RuntimeException("no prior");
                }
                leaf = leaf.left;
                idx = leaf.keys.size() - 1;
            }
        } while(leaf.duplicate.get(idx) == 0);
        return leaf.values.get(idx);
    }

    public V successor(K key) {
        SearchResult<K, V> r = root.find(key);
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);

        int idx = r.idx;
        if(!r.found()) {
            idx = -(idx+1);
        } else {
            idx++;
        }

        if (idx == leaf.keys.size()) {
            if (leaf.right == null) {
                throw new RuntimeException("no successor");
            }
            leaf = leaf.right;
            idx = 0;
        }

        while(leaf.duplicate.get(idx) == 0) {
            idx++;
            if (idx == leaf.keys.size()) {
                if (leaf.right == null) {
                    throw new RuntimeException("no successor");
                }
                leaf = leaf.right;
                idx = 0;
            }
        }

        return leaf.values.get(idx);
    }

    public void delete(SearchResult<K, V> sr) {
        if (!sr.found()) {
            throw new RuntimeException("no key to del");
        }

        Leaf<K, V> leaf = (Leaf<K, V>)sr.path.get(sr.path.size()-1);
        DeleteResult<K, V> dr = leaf.delete(sr);
        if (dr.root != null) {
            root = dr.root;
        }
    }

    public int rank(K key) {
        SearchResult<K, V> r = root.find(key);
        if (!r.found()) {
            throw new RuntimeException("no key to rank");
        }
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);
        int rank = 1;
        for (int i = 0; i < r.idx; i++) {
            rank += leaf.duplicate.get(i);
        }

        for(int i = 0;i < r.path.size()-1;i++) {
            Interior<K, V> interior = (Interior<K, V>)r.path.get(i);
            int idx = r.idxs.get(i);
            if (idx < 0) {
                idx = -(idx + 1);
            }
            for (int j = 0; j <= idx-1; j++) {
                rank += interior.children.get(j).total;
            }
        }

        return rank;
    }

    public V val_at(int rank) {
        if (rank < 1 || rank > root.total) {
            throw new RuntimeException("rank out of range");
        }

        Node<K, V> h = root;
        while(h.get_type() != LEAF) {
            Interior<K, V> interior = (Interior<K, V>)h;
            int idx = 0;
            while(idx < interior.keys.size() && rank > interior.children.get(idx).total) {
                rank -= interior.children.get(idx).total;
                idx++;
            }
            h = interior.children.get(idx);
        }

        Leaf<K, V> leaf = (Leaf<K, V>)h;
        int idx = 0;
        while(idx < leaf.keys.size() && rank > leaf.duplicate.get(idx)) {
            rank -= leaf.duplicate.get(idx);
            idx++;
        }

        return leaf.values.get(idx);
    }

    public void print_all_keys() {
        Node<K, V> h = root;
        while(h.get_type() != LEAF) {
            Interior<K, V> interior = (Interior<K, V>)h;
            h = interior.children.get(0);
        }
        Leaf<K, V> leaf = (Leaf<K, V>)h;
        while(leaf != null) {
            for(int i = 0;i < leaf.keys.size();i++) {
                System.out.print(leaf.keys.get(i) + "(" + leaf.duplicate.get(i) + ") ");
            }
            leaf = leaf.right;
        }
        System.out.println();
    }

    public int check_total(Node<K, V> h) {
        if (h.get_type() == LEAF) {
            int total = 0;
            total = ((Leaf<K, V>)h).duplicate.stream().mapToInt(Integer::intValue).sum();
            if (total != h.total) {
                throw new RuntimeException("total not match");
            }
            return total;
        }
        int total = 0;
        Interior<K, V> itr = (Interior<K, V>)h;
        for (int i = 0; i < itr.children.size(); i++) {
            total += check_total(itr.children.get(i));
        }
        if (total != h.total) {
            throw new RuntimeException("total not match");
        }
        return total;
    }

    public static void main(String[] argv) throws IOException {
//        long start = System.currentTimeMillis();

        BPlusTree<Integer, Integer> tree = new BPlusTree<>();

//        for(int i = 10;i > 0;i--){
//            tree.insert(i, i);
//            tree.print_all_keys();
//        }
//
//        System.out.println("---------------------------");
//        for (int i = 1; i <= 10; i++) {
//            System.out.println("del " + i);
//            SearchResult<Integer, Integer> sr = tree.find(i);
//            tree.delete(sr);
//            tree.check_total(tree.root);
//            tree.print_all_keys();
//        }

        Scanner in = new Scanner(System.in);

//        Scanner in = new Scanner(new FileInputStream("test_data\\P3369_8.in"));
//        System.setOut(new PrintStream(new FileOutputStream("new.txt")));
//
        int n = in.nextInt();
        for(int i = 0;i < n;i++) {
            int opt = in.nextInt();
            int x = in.nextInt();

            if (opt == 1) {
                tree.insert(x, x);
            } else if (opt == 2) {
                SearchResult<Integer, Integer> sr = tree.find(x);
                tree.delete(sr);
            } else if (opt == 3) {
                int rank = tree.rank(x);
                System.out.println(rank);
            } else if (opt == 4) {
                int val = tree.val_at(x);
                System.out.println(val);
            } else if (opt == 5) {
                int p = tree.prior(x);
                System.out.println(p);
            } else if (opt == 6) {
                int s = tree.successor(x);
                System.out.println(s);
            } else {
                throw new RuntimeException("error opt");
            }

        }

//        long end = System.currentTimeMillis();
//        System.err.println("time = " + (end - start) + "ms");
    }

}
