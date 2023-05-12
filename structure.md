# Database file format

## File header

> See code



## B+ tree

BNode

| len    | name        | description              |
| ------ | ----------- | ------------------------ |
| 1      | type        | page type, = PAGE_B_NULL |
| 4      | hdr_size    | header size              |
| 4      | father      | page number of father    |
| 4      | key_count   |                          |
| 4      | cell_size   |                          |
| 4      | cell_count  |                          |
| 4      | free_cell   | free cell head           |
| 4      | key_count   | key count in this table  |
| 4      | value_count |                          |
| var(4) | key_types   |                          |
| var(4) | value_types |                          |

* hdr_size: 在建表时确定，是所有 B 树节点类型的 header size 最大值 + 储存类型信息所需空间 + cell_ptrs
* key_count、key_types、value_count、value_types 在建表时确定，每个节点都储存一次，便于测试



free cell:

| len  | name      | desc                  |
| ---- | --------- | --------------------- |
| 1    | cell_type | = CellType.FREE       |
| 4    | next      | next free cell offset |



BInteriorNode

接上 BNode 的结构

| len    | name       | desc                          |
| ------ | ---------- | ----------------------------- |
| 4      | total      | total entry count in the node |
| 4      | tail_child | the last child page number    |
| var(4) | cell_ptrs  | offsets to key cells          |



BInterior cell:

* key cell:

  | len  | name       | desc              |
  | ---- | ---------- | ----------------- |
  | 1    | cell_type  | = CellType.KEY    |
  | 4    | child_page | child page number |
  | var  | key        | key data          |





BLeaf

| len    | name          | desc                 |
| ------ | ------------- | -------------------- |
| 4      | overflow_page | overflow page number |
| 4      | left_sibling  |                      |
| 4      | right_sibling |                      |
| var(4) | cell_ptrs     |                      |

* leaf cell

  | len  | name            | desc                          |
  | ---- | --------------- | ----------------------------- |
  | 1    | type            | CellType.LEAF                 |
  | 4    | overflow_offset | value offset in overflow page |
  | var  | key             | key data                      |

* leaf cell 中也只储存 key，把 values 全部放在 overflow page 中
* overflow page 被视为一个无限大的储存空间，overflow page 内部 API 自动完成分页、空间管理功能。
  * 难点：向 varchar 这种长度可变的字符串如何处理？不处理，全部使用定长字符串
  * overflow page 内部空间管理
    * 用一个 OverflowPageManager 类基于原有的 overflow page 进行管理



