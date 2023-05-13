# Database file format

## 总览

该数据库模仿 SQLite 采用单文件、页式储存模式。每一页必须是下述页类型之一：

| name                 | type | description                                                  |
| -------------------- | ---- | ------------------------------------------------------------ |
| database_header_page | 无   | 文件前 HEADER_SIZE 字节视为特殊的一页。HEADER_SIZE 在代码编译时确定，一般取为 128 |
| NULL                 | 0    | 空页，没有任何实际意义，一个 consistence 的 database file 不应该出现 NULL page |
| FREE                 | 1    | 空闲页，下次 allocate page 时优先使用空闲页                  |
| OVERFLOW             | 2    | 溢出页，且形成 overflow page chain                           |
| BTREE_NULL           | 3    | BTree null page.                                             |
| BTREE_INTERIOR       | 4    | BTree interior node page                                     |
| BTREE_LEAF           | 5    | BTree leaf node page                                         |

每一页都有规定有 header 和 body。具体结构见下文。



对于任意的表，将所有 primary_key 视为 key，所有 non-primary key 视为 value，可将表一张二维表视为 key --> value 的映射，一个 （key，value）称为一个 entry，统一使用来维护这个映射。

在本项目的 B+ tree 中，一个 tree node 对应 database file 中独立的一页。interior node 值储存 key 且将 key 储存在页内。leaf node 在页内储存 key，将所有 value 单独存放到一个 overflow page chain 中。这样能使树尽可能矮，同时 value 倾向于储存在硬盘中连续的位置，提高效率。





## database_header_page

定义如下：

| len  | name           | def_value        | description                                            |
| ---- | -------------- | ---------------- | ------------------------------------------------------ |
| 32   | file_id        | "SQLite-like-db" | file type identifier                                   |
| 2    | ver            | 1                | database version                                       |
| 1    | page_size      | 7, for debug     | assume the value is N, the the real page size is (2^N) |
|      |                | 12, for product  |                                                        |
| 4    | page_count     | 1                | total page count, including free page count            |
| 4    | freelist_head  | 0                | the head free page id                                  |
| 4    | freelist_count | 0                | free page count                                        |
| 4    | cache_count    | 4096             | maximum pages in cache                                 |
| 4    | root_table     | 1                | page number of the root of table `__db_schema`         |
|      |                |                  | **NOT IMPLEMENTED YET**                                |

* `__db_schema` 是储存所有表的 meta_info 的表



## free page chain

当一个页不再使用，被释放后，会成为 free page 类型，free page 类型的 page。所有 free page 被串成一个链表，当数据库需要新分配一页的磁盘空间时，优先重新使用 free page 对应的磁盘空间。



free page 结构定义如下：

headers 定义：

| len  | name      | description               |
| ---- | --------- | ------------------------- |
| 1    | type      | page type                 |
| 4    | next_free | page id of next free page |

body 定义：无意义



## overflow chain

当向 overflow page 中写入数据时，我们希望能等效地得到一个无限大的可写入空间，所以 overflow page 也具备链表的结构，同时在软件层面为指向 overflow page 的 IO 做一层抽象，自动处理“溢出页的溢出页”，对于应用者来说就好像直接写入到一个文件一样。

因为一个溢出页自身也可能有溢出页，所以将这样形成的链表称为 overflow chain。

overflow page headers 定义：

| len  | name | description                                                  |
| ---- | ---- | ------------------------------------------------------------ |
| 1    | type | page type                                                    |
| 4    | next | the page number of the overflow page that hold the data that overflow this overflow page |

overflow page body 定义：任意用户定义的数据

IO 抽象层：

* `OverflowPage.OutputStream` 继承自标准 OutputStream。当写入数据过多时，会自动申请新的一个 overflow page 存放数据。
* `OverflowPage.InputStream` 继承自标准 InputStream。自动处理数据跨页问题。

使用上述 IO 抽象层，即可像使用 `FileOutputStream` 一样储存数据，为后续应用提供便利。同时，需要储存超大对象时，流式传输更能满足硬件限制。



## cache 机制

所有缓存管理类需要实现接口 `Cache` 中定义的如下方法：

* `byte[] read(int page_id, int pos, int length);`
* `void write(int page_id, int pos, byte[] data, int offset, int length);`

表示在使用缓存的前提下，读、写 database file 数据。目前实现 cache 管理类如下：

* `NoCache`：无缓存，直接读写文件
* `LRUCache`：LRU 缓存管理策略



## B+ tree

### Payload

B+ tree 储存 （key，value），例如对于下述 SQL 建表语句：

```SQL
create table student_info (
    class_id int,
    name varchar(32),
    age int,
    gpa float,
    address varchar(32),
    primary key (class_id, name)
);
```

* key = (class_id, name)
* value = (age, float, address)

可以发现 key、value 都是特定类型对象的集合，所以使用 payload 来描述这种特定对象的集合。payload 对于代码中 `Payload` 类，具有如下属性：

* `List<Integer> types`, 描述 payload 中对象的数据类型
* `byte[] data`，这些对象的二进制数据
* 两个 types 相同的 payload 是可比较的，按照字典序进行比较。



对象数据类型定义如下（来源于 `ObjType` 类）：

| java 类型 | name      | 类型编码                      |
| --------- | --------- | ----------------------------- |
| int       | INT       | 1                             |
| long      | LONG      | 2                             |
| float     | FLOAT     | 3                             |
| string    | STRING(N) | N + 16, 表示最长为 N 的字符串 |



### B+ tree 节点结构

#### 基本概念

B+ tree 的 interior 节点储存一个 key 数组和指向子节点的指针（即子节点 page number）。

B+ tree 的 leaf 节点需要储存一个 key 数组并维护好找到 value 位置的信息（即 overflow page number 和 value 在 overflow page 中的位置），还需要储存左相邻叶子节点的 page number。

key 数组、value 数组、子节点指针都是动态可变的，所以采用 slot-cell 结构储存，两种节点都需要维护 slot 相关信息。Cell 的大小由 key、value 中的对象类型完全确定，是定长的。所以在 page body 中可以从前往后排列 cell。为了管理被释放后的 cell 空间，引入 free cell 类型。

无论是 interior 节点还是 leaf 节点，为了便于实现和调试，在每一个节点中都储存一次 key_types 和 value_types 信息。同时，还需要维护父节点 page number。



综上，B+ tree 节点结构定义如下：



#### B+ tree page headers structure

| len               | interior      | leaf          | description              |
| ----------------- | ------------- | ------------- | ------------------------ |
| 1                 | type          |               | page type, = PAGE_B_NULL |
| 4                 | hdr_size      |               | header size              |
| 4                 | father        |               | page number of father    |
| 4                 | cell_size     |               |                          |
| 4                 | cell_count    |               |                          |
| 4                 | free_cell     |               | free cell head           |
| 4                 | key_count     |               | object count in key      |
| 4                 | value_count   |               | object count in value    |
| 4                 | total         | overflow_page |                          |
| 4                 | tail_child    | left_sibling  |                          |
| 4                 | _not\_used_   | right_sibling |                          |
| 4 * key_count     | key_types     |               |                          |
| 4 * value_count   | value_types   |               |                          |
| 4                 | slot_capacity |               |                          |
| 4                 | slot_count    |               |                          |
| 4 * slot_capacity | slots         |               |                          |

* leaf 列值为空意为同 interior 列
* 上表中 `hdr_size`、`key_count`、`value_count`、`slot_capacity` 的值在建表时确定，随后添加、删除、修改数据都不改变这些字段的值

* `key_count` = primary key 列的数量
* `value_count` = non-primary key 列的数量
* `slot_capacity` 最大储存槽数量，有 page size 和表的属性大小综合决定
* `not_used`：为了对齐 interior、leaf 的 header 引入的字段，无实际含义



#### interior node body structure

interior node body 由多个 cell 组成，cell 可以是 interior cell，结构为：

| len                | name       | description         |
| ------------------ | ---------- | ------------------- |
| 1                  | type       | cell type           |
| 4                  | child_page | child page number   |
| size_of(key_types) | data       | binary data for key |



也可以是 free cell，结构为：

| len                | name | description       |
| ------------------ | ---- | ----------------- |
| 1                  | type | cell type         |
| 4                  | next | next free cell id |
| size_of(key_types) |      | not used          |



####  leaf node body structure

leaf node body 由多个 cell 组成，cell 可以是 leaf cell 类型，结构为：

| len                | name    | description                        |
| ------------------ | ------- | ---------------------------------- |
| 1                  | type    | cell type                          |
| 4                  | unit_id | unit id for value in overflow page |
| size_of(key_types) | data    | binary data for key                |

也可以是 free cell，不再赘述。



####  (key, value) 中 value 的储存方式

为了压缩树的高度，我们的设计中不在 leaf node page 中储存 value，而是单独使用一个 overflow page chain 来储存所有 value。

每个 leaf node 都会关联一个 overflow page（headers 中的`overflow_page`）字段，所有 value 都会储存在这个 overflow page 代表的 overflow chain 中。

因为建表时已经确定了 value 占用的空间大小，即 size_of(value_types)，所以可以直接将 overflow chain 的空间按照 size_of(value_types) 划分为许多个 unit，使用 unit id 来标明位置。申请、释放空间都以定长的 unit 为单位，大大简化实现难度。

相关代码实现在 `CellStorage` 类中。



####  high level API

B+ tree 的高层次 API 实现在 `BPlusTree` 类中，支持：

* 查找 key
* 删除
* 添加
* 遍历
* 高效计数（未实现）
* 查询 key 的前驱、后继（未实现）
