sequenceDiagram
    User->>+Map: Insert(batch)
    Map->>+Hasher: Hash(batch)
    Hasher->>+Map: Hashes
    Map->>+RowEncoder: Insert(batch)
    RowEncoder->>+Storage: Store(output stream)
    RowEncoder->>+Map: Row IDs
    Map->>+SwissTable: Insert(hashes, Row IDs)
    SwissTable->>+Storage: Store(random access)
    SwissTable->>+Map: -
    Map->>+User: -
