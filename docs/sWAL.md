# Simple WAL design

为了简化设计，只支持单个事务



## 基本流程

事务流程

1. DBFile.transaction
   * 这个 DBFile 禁止 write 操作
   * 新建并返回一个特殊的 TDBFile，这个类继承自 DBFile 但行为有所不同
     * 创建 WAL 缓存、WAL 文件并标记为 "rollback"
     * 创建 WAL table：(page num, wal page num)
     * WAL 缓存是 WAL 文件的缓存
       * wal page num --> wal page data 
     * TDBFile.read 重定向：
       * 若 WAL table 含有 page num，使用 wal page num 的数据
       * 若 wal cache 不含 page num，使用 DBFile.cache 中 page num 的数据
     * TDBFile.write 重定向：
       * 若 WAL table 含有 page num，用 new data 覆盖 wal page num 中的数据
       * 若 WAL table 不含 page num，从 DBFile.cache 复制 page num data 到 wal cache 并覆盖
     * 要求上层用户在 transaction 内部使用 TDBFile 来进行各种 search、update、insert 等操作
2. WAL 缓存有容量限制，把超出容量的 page 写入 WAL 文件
   * WAL 文件写入记录的逻辑结构为 (page num, old data, new data)
   * old data 从 DBFile 中 read 读取出来，new data 是 WAL cache
3. TDBFile.commit
   * 把 WAL 缓存全部写入 WAL 文件，把 WAL 文件标记为 "committing"
   * 根据 WAL 文件中的记录，把 new data 全部写入 DBFile 文件
   * 把 WAL 文件标记为 "committed" （或直接删除？）



rollback：

* 直接抛弃 TDBFile ，不调用 commit 即可



重启恢复：

* 若 WAL 文件标记为 "rollback"，直接删除 WAL 文件
  * 对应数据库在执行事务过程中崩溃，使用 undo 恢复数据库
* 若 WAL 文件标记为 "committing"，
  * 把 WAL 记录中所有 new data 写入 DBFile
  * 把 WAL 标记为 "committed"
  * 对应数据库在 commit 事务时崩溃，使用 redo 恢复数据库
* 若 WAL 文件被标记为 "committed"
  * 此时数据库数据是完整、一致的，直接清空or删除 WAL 文件



##  sWAL 文件结构

sWAL 文件也是不支持 transaction 的 DBFile，称为 wal_db，

* wal_db 页大小同原 db
* wal_db 拓展一个 db header，"wal_status" 储存 "committed" 等标识



wal_db 建立一张表 records: (page_num, odp, ndp)

* odp, old data page 指向储存老数据的 data page
* ndp, new data page 指向储存新数据的 data page

DBFile 拓展一个特殊的页：RAWPage，无 page header，仅供 WAL 使用









