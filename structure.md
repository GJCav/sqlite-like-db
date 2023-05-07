# Database file format

## File header


## BTreeRootPage

| len    | name           | value                                               |
|--------|----------------|-----------------------------------------------------|
| 1      | page type      | = 0                                                 |
| 4      | payload_size   | set based on an algorithm                           |
| 4      | column_count   |                                                     |
| 4      | free_cell_head | offset to the first free cell                       |
| 4      | tail_ptr       | rightmost child pointer of BTree node               |
| 4      | cell_count     | active cell count                                   |
| var(4) | column_types   |                                                     |
| var(1) | is_key         | boolean, true if this column belongs to primary key |
| var(4) | cell_ptrs      | offset to cells                                     |
| var    | \<free_space>  | free space                                          |
| var    | \<cells>       | cells                                               |
