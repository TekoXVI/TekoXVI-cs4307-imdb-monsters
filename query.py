from btree import Database, print_page, report
from step import step_table, step_index
from typing import Any, Optional, Tuple, Dict

# your code goes here
def getCatalog(db: Database) -> Dict[str, int]:
    table = step_table(db, 1, 0)
    dict: Dict[str, int] = {}
    for (rowid, row) in table:
        dict[row[1]] = row[3]
    return dict

def scanForTitle(db: Database, catalog: Dict[str, int], title: str) -> None:
    table = step_table(db, catalog['titles'], 0)
    for (rowid, row) in table:
        if row[3] == title:
            print(f'rowid: {rowid}, row: {row}')

def stabForTitle(db: Database, catalog: Dict[str, int], title: str) -> None:
    index = step_index(db, catalog['titles_by_primary_title'], (title, ))
    for (key, row) in index:
        if row[0] != title:
            break
        table = step_table(db, catalog['titles'], row[1])
        (rowid, r) = next(table)
        print(f'rowid: {rowid}, row: {r}')

def findPeople1(db: Database, catalog: Dict[str, int], title: str) -> None:
    titles = step_table(db, catalog['titles'], 0)
    for (title_id, row) in titles:
        t_id = title_id
        if row[3] == title:
            crew = step_table(db, catalog['crew'], 0)
            for (rowid, row2) in crew:
                if row2[0] == t_id:
                    person = step_table(db, catalog['people'], row2[1])
                    (rid, r) = next(person)
                    print(f'rowid: {rid}, row: {r}')

def findPeople2(db: Database, catalog: Dict[str, int], title: str) -> None:
    crew = step_table(db, catalog['crew'], 0)
    for (rowid, row) in crew:
        person_id = row[1]
        title_id = row[0]
        titles = step_table(db, catalog['titles'], title_id)
        (rowid, row) = next(titles)
        if row[3] == title:
            people = step_table(db, catalog['people'], person_id)
            (rowid, row) = next(people)
            print(f'rowid: {rowid}, row: {row}')

def findPeopleIndex(db: Database, catalog: Dict[str, int], title: str) -> None:
    titles = step_index(db, catalog['titles_by_primary_title'], (title, ))
    for (p_title, row) in titles:
        title_id = row[1]
        if p_title[0] != title:
            break

        crew = step_index(db, catalog['crew_by_title_and_person'], (title_id,))
        for (rowid, row) in crew:
            t_id = row[0]
            p_id = row[1]
            r_id = row[2]
            if t_id != title_id:
                break

            person = step_table(db, catalog['crew'], r_id)
            (row_id, row) = next(person)
            person_id = row[1]

            record = step_table(db, catalog['people'], person_id)
            (rowids, row) = next(record)
            print(f'rowid: {rowids}, row: {row}')

def findPeopleCoveringIndex(db: Database, catalog: Dict[str, int], title: str) -> None:
    titles = step_index(db, catalog['titles_by_primary_title'], (title, ))
    for (p_title, row) in titles:
        title_id = row[1]
        if p_title[0] != title:
            break

        crew = step_index(db, catalog['crew_by_title_and_person'], (title_id,))
        for (rowid, row) in crew:
            t_id = row[0]
            p_id = row[1]
            r_id = row[2]
            if t_id != title_id:
                break
            
            person = step_table(db, catalog['people'], p_id)
            (rowids, row) = next(person)
            print(f'rowid: {rowids}, row: {row}')
            