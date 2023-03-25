from btree import *
from typing import Tuple, Any, Iterator

# Copy your implementation of the btree iterator code here
# Note: the function types given here have been updated to be a little more precice.
# Use these types but your existing code.

def step_table(db: Database, root: int, key: int) -> Iterator[Tuple[int, Tuple[Any, ...]]]:
    inc_table_scans()
    def helper(db: Database, root: int, key: int) -> Iterator[Tuple[int, Tuple[Any, ...]]]:
        page = db.load_page(root)
        assert(page.page_type == TABLE_LEAF or page.page_type == TABLE_INTERIOR)
        for cell in page.cells:
            inc_rows_scanned()
            if cell.rowid >= key:
                if cell.page_type == TABLE_INTERIOR:
                    left = cell.left_child
                    yield from helper(db, left, key)
                else:
                    inc_rows_returned()
                    yield (cell.rowid, cell.fields)
        if cell.page_type == TABLE_INTERIOR:
            right = page.right_child
            yield from helper(db, right, key)
    yield from helper(db, root, key)

def step_index(db: Database, root: int, key: Tuple[Any, ...]) -> Iterator[Tuple[Tuple[Any, ...], Tuple[Any, ...]]]:
    inc_index_scans()
    def helper(db: Database, root: int, key: Tuple[Any, ...]) -> Iterator[Tuple[Tuple[Any, ...], Tuple[Any, ...]]]:
        page = db.load_page(root)
        assert(page.page_type == INDEX_LEAF or page.page_type == INDEX_INTERIOR)
        for cell in page.cells:
            inc_rows_scanned()
            if cell.fields[:len(key)] >= key:
                if cell.page_type == INDEX_INTERIOR:
                    left = cell.left_child
                    yield from helper(db, left, key)
                inc_rows_returned()
                yield (cell.fields[:len(key)], cell.fields)
        if cell.page_type == INDEX_INTERIOR:
            right = page.right_child
            yield from helper(db, right, key)
    yield from helper(db, root, key)
