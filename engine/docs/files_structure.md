## coDB files structure

All coDB files are stored in a special directory named `.coDB`.  
- On Linux, this is located at `~/.coDB`.
- On Windows, it is located at `%APPDATA%\.coDB`.


### Structure

At the top level inside the `.coDB` directory are subdirectories for each database. For example, `database-A` contains data for the database named "database-A":

```text
.coDB
    /database-A
    /database-B
    /database-C
```

Inside each database directory, there is a metadata file (`metadata.coDB`) and directories for each table:

```text
.coDB
    /database-A
        metadata.coDB
        /table-A
        /table-B
        /table-C
```

Each table directory contains two files:
- `table_name.idx`: Stores a B-tree index of the table's keys.
- `table_name.tbl`: Stores table rows using slotted pages.

```text
.coDB
    /database-A
        metadata.coDB
        /table-A
            table-A.idx
            table-A.tbl
```

### Assumptions

Files used in coDB must match the exact layout described in this documentation. Any deviation can result in unexpected behaviour. Currently, changing the location of the `.coDB` directory is not supported, but this may change in the future.