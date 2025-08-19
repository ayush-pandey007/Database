## SimpleDatabase — A paged B-tree storage engine in Java


### Why this project
A compact, from-scratch storage engine that demonstrates real systems concepts:
- **Paged I/O** with 4KB pages over `RandomAccessFile`
- **On-disk formats** using `ByteBuffer` serialization
- **B-tree indexing**: binary search, ordered inserts, leaf splits, and parent updates
- **REPL** with `insert`/`select` to interactively manipulate data


This is ideal to showcase practical understanding of databases, filesystems, and performance-aware programming.


---


## Features
- **Fixed page size**: 4096 bytes per page
- **Row format**: `id:int`, `userName:fixed 32B`, `email:fixed 255B`
- **Leaf nodes**: sorted by key; linked via `nextLeaf` for fast full scans
- **Internal nodes**: store guide keys and child pointers for O(log n) lookup
- **Binary search** in leaves/internal nodes
- **Duplicate key detection**
- **Leaf splitting** with redistribution and parent/root update
- **Pager cache** with flush-on-close durability


Limitations (by design, to keep code small):
- Internal-node splits (beyond the root) are not implemented yet
- No WAL/transactions; durability via close/flush


---


## Quick start


### Prerequisites
- Java 8+ (JDK)


### Build & Run
- Windows PowerShell
```powershell
javac *.java
java SimpleDatabase database.db
```
- macOS/Linux
```bash
javac *.java
java SimpleDatabase database.db
```
If no filename is provided, it defaults to `database.db`.


### REPL usage
- Prompt: `db > `
- Commands:
  - Meta: `.exit`
  - Statements: `insert <id> <username> <email>`, `select`


Example session
```text
db > insert 1 ayush ayush@email
db > insert 2 bob bob@email
db > select
(1, ayush, ayush@email)
(2, bob, bob@email)
db > .exit
```


---


## Project structure
- `SimpleDatabase.java` — REPL, parsing, command execution
- `Table.java` — database handle; owns `Pager` and root page number
- `Pager.java` — page cache + file I/O (`RandomAccessFile`); 4KB pages
- `Cursor.java` — table iteration (start, next), value access
- `Node.java` — B-tree node ops (leaf/internal layout, search, insert, split)
- `Row.java` — in-memory row (id, userName, email)
- `Constant.java` — sizes and layout constants


---


## How it works (high level)
1. Start the REPL (`SimpleDatabase.main`), open table and pager; create page 0 as a leaf if file is empty
2. For each statement:
   - `insert`: parse → find leaf/slot via B-tree → write key + serialized row → split if leaf is full → update parent/root
   - `select`: start at leftmost leaf, iterate across leaves via `nextLeaf`, deserialize and print rows
3. On `.exit`: flush all dirty pages to disk and close the file


---


## On-disk format (essentials)


### Sizes
- `PAGE_SIZE = 4096`
- Row value size: `ROW_SIZE = 4 (id) + 32 (userName) + 255 (email) = 291 bytes`
- Leaf cell: `KEY(4) + VALUE(291) = 295 bytes`
- Leaf header: 14 bytes → space for cells: `4096 - 14 = 4082` → max cells per leaf: `4082 / 295 = 13`


### Leaf node layout
```text
[ header ]  = nodeType | isRoot | parentPtr | numCells | nextLeaf
[ cells ]   = (key:int, value:fixed ROW_SIZE bytes)*
```


### Internal node layout
```text
[ header ]  = nodeType | isRoot | parentPtr | numKeys | rightChild
[ cells ]   = (childPage:int, key:int)*  // key[i] = max key in child[i]
Routing: if searchKey ≤ key[i] → child[i], else → rightChild
```


---


## Leaf split — what happens
Insert into a full leaf triggers a split: redistribute old+new keys in order across two leaves, link them, and update the parent.


Before (full):
```text
Leaf A: [ 10 | 20 | 40 ]   (cap=3)
Insert: 30
```
Split & link:
```text
A (left): [ 10 | 20 ]   → next →   C (right): [ 30 | 40 ]
```
Promote separator to parent (the road sign):
```text
separator = max(A) = 20
Parent (root if needed): [ child0=A | key0=20 | right=C ]
```
Search routing:
- `≤ 20` → A
- `> 20` → C


Full-table scans follow leaf links A → C → ...


---


## Internals: Pager and ByteBuffer
- `Pager.getPage(n)`
  - If cached: return a `duplicate()` view of the same bytes (independent position)
  - Else: allocate 4KB buffer; if `n < numPages`, `seek(n * 4096)` and read into `buffer.array()`; cache it
  - Updating `numPages` if allocating beyond current end
- `Pager.flush(n)` writes the cached buffer back to `n * 4096`
- Serialization uses `ByteBuffer.putInt(...)` and fixed-size UTF‑8 with zero padding


---


## Known limitations / roadmap
- **Internal node split (beyond root):** not implemented — tree height currently grows to 2 (root internal + leaves). Next steps:
  - Track parent pointers per node
  - Implement internal split and upward propagation; split root when full
- **Durability:** flush-on-close only; consider `FileChannel.force(true)` or a WAL for crash safety
- **Input validation:** minimal; consider stricter parsing and bounds checks


---


## Tips for demo
- Insert enough rows to show a leaf split (≥14 inserts for one leaf)
- Show ordered `select` traversal across leaves
- Point at `Constant.java` to explain sizes and capacities
- Walk through the split diagram above


---


## Acknowledgements
This is an educational project inspired by how real databases (like SQLite) structure pages and B‑trees, built from scratch in Java for clarity.
