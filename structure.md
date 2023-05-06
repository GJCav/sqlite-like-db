# Database file format

## File header

| offset | len | alias          | field                                                                                                                          |
|--------|-----|----------------|--------------------------------------------------------------------------------------------------------------------------------|
| 0      | 32  | file_id        | the header string "SQLite-like db\0"                                                                                           |
| 32     | 2   | ver            | the database version                                                                                                           |
| 36     | 1   | page_size      | an unsigned byte. set the size of database page size. must be between 10 and 20 note the value as V, then the page size is 2^V |
| 37     | 4   | freelist_head  | the page number of the first freelist page                                                                                     |
| 41     | 4   | freelist_count | total number of freelist pages                                                                                                 |
|        |     | else           | not used                                                                                                                       |

