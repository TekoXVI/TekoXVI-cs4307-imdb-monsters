Performing the join using a covering index
------------------------------------------

Write a new function in `query.py` with the following name and type:

    def findPeopleCoveringIndex(db: Database, catalog: Dict[str, int], title: str) -> None:

Recall that you have two indices to help:

    CREATE INDEX titles_by_primary_title ON titles (primary_title);
    CREATE INDEX crew_by_title_and_person ON crew (title_id, person_id);

This time you will take advantage of the fact that
`crew_by_title_and_person` is a covering index for the purposes of
this query:

*   Use an index lookup to find the title ID.

*   For each match, do an index lookup (using the title ID) to find
    the person IDs linked to the title via the crew table. For this
    you will search by the title ID but then ignore the crew rowid
    (since you do not actually need any fields from the crew table)
    and just use the person ID that is also in the index record.

*   Using the person ID from the crew index, perform a stabbing
    query to find the matching person record and print it out in the
    same format as before.
