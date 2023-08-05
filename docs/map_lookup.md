sequenceDiagram
    User->>+Map: Lookup(ProbeBatch)
    Map->>+Hasher: Hash(ProbeBatch)
    Hasher->>+Map: ProbeHashes
    Map->>+SwissTable: Lookup(ProbeHashes)
    SwissTable->>Storage: Read(random access)
    SwissTable->>+Map: Row IDs
    Note right of Map: May be more row IDs than input rows<br/>If there are multiple matches for a hash<br/>Also, some row IDs may be null.
    Map->>+RowDecoder: Decode(Row IDs)
    RowDecoder->>Storage: Read(random access)
    RowDecoder->>+Map: PayloadBatch
    Map->>+Equality: Filter(ProbeBatch, Row IDs, PayloadBatch)
    Note right of Equality: This step is to handle keys<br/>with the same hash.  Can skip<br/>or use basic binary equality to start.
    Equality->>+Map: PayloadBatch (filtered)
    Map->>+User: PayloadBatch
    Note right of User: Payload batch may be smaller/larger<br/>than probe batch.  May need to also<br/>also return row IDs to help merge
