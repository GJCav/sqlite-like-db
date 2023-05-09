package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BPlusTree<K extends Comparable<K>, V> {
    public static int MIN_DEG = 3;
    public static int MAX_DEG = 5;
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

    public static abstract class Node<K extends Comparable<K>, V> {

        public Node<K, V> father;

//        public int idx_in_father = 0; // 维护这个会导致过多的磁盘IO，所以不维护了

        public List<K> keys = new ArrayList<>();
        int total = 0;

        public int get_type() { throw new RuntimeException(); }

        public int get_total() { return total; }

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
        public abstract Node<K, V> merge();
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
                this.total += pseudo.left.total + pseudo.right.total;
            }
        }

        /**
         *
         * @return new root node if root has been split, otherwise null
         */
        @Override
        public SplitResult<K, V> split() {
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

            Node<K, V> root = null;
            if (father != null) {
                if (father.keys.size() + 1 > MAX_DEG) {
                    root = father.split().root;
                }
                ((Interior<K, V>)father).insert(pseudo);
            } else {
                root = pseudo.to_interior();
            }

            SplitResult<K, V> r = new SplitResult<>();
            r.left = this;
            r.right = right;
            r.key = key;
            r.root = root;
            return r;
        }

        @Override
        public Node<K, V> merge() {
            return null;
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
        }

        @Override
        public SplitResult<K, V> split() {
            Leaf<K, V> right = new Leaf<>();

            int mid = keys.size() / 2;
            K key = keys.get(mid);

            right.keys.addAll(keys.subList(mid+1, keys.size()));
            right.values.addAll(values.subList(mid+1, values.size()));
            right.duplicate.addAll(duplicate.subList(mid+1, duplicate.size()));
            right.total = right.duplicate.stream().mapToInt(i -> i).sum();

            this.keys.subList(mid, keys.size()).clear();
            this.values.subList(mid, values.size()).clear();
            this.duplicate.subList(mid, duplicate.size()).clear();
            this.total = this.duplicate.stream().mapToInt(i -> i).sum();

            this.right = right;
            right.left = left;

            PseudoInterior<K, V> pseudo = new PseudoInterior<>(this, key, right);
            Node<K, V> root = null;
            if (father != null) {
                if (father.keys.size() + 1 > MAX_DEG){
                    root = father.split().root;
                }
                ((Interior<K, V>)father).insert(pseudo);
            } else {
                root = pseudo.to_interior();
            }

            SplitResult<K, V> r = new SplitResult<>();
            r.left = this;
            r.right = right;
            r.key = key;
            r.root = root;
            return r;
        }

        @Override
        public Node<K, V> merge() {
            return null;
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

    public V prior(K key) {
        SearchResult<K, V> r = root.find(key);
        if (!r.found()) {
            throw new RuntimeException("no key");
        }
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);
        int idx = r.idx;

        while(leaf.duplicate.get(idx) == 0) {
            idx--;
            if (idx == -1) {
                if (leaf.left == null) {
                    throw new RuntimeException("no prior");
                }
                leaf = leaf.left;
                idx = leaf.keys.size() - 1;
            }
        }
        return leaf.values.get(idx);
    }

    public V successor(K key) {
        SearchResult<K, V> r = root.find(key);
        if (!r.found()) {
            throw new RuntimeException("no key");
        }
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);
        int idx = r.idx;

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

    public void pseudo_delete(K key) {
        SearchResult<K, V> r = root.find(key);
        _pseudo_delete(r);
    }

    public void _pseudo_delete(SearchResult<K, V> r) {
        if (!r.found()) {
            throw new RuntimeException("no key to del");
        }
        Leaf<K, V> leaf = (Leaf<K, V>)r.path.get(r.path.size()-1);
        if(leaf.duplicate.get(r.idx) == 0) {
            throw new RuntimeException("no key to del 1");
        }

        leaf.duplicate.set(r.idx, leaf.duplicate.get(r.idx) - 1);
        Node<K, V> h = leaf;
        while(h != null) {
            h.total--;
            h = h.father;
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
            for (int j = 0; j < idx-1; j++) {
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


    public static void main(String[] argv) {
        BPlusTree<Integer, Integer> tree = new BPlusTree<>();
        for(int i = 0;i < 100;i++) {
            tree.insert(i, i);
        }
    }

}
