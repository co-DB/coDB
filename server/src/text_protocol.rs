use serde::{Deserialize, Serialize};

/// This file contains definitions of all requests and responses in the text protocol of coDB.
/// The type of request/response is distinguished by the appropriately named 'type' field in the
/// received or sent message.
///
/// For every query the responses follow the same sequence:
/// 1. `Acknowledge` - Query received and execution started
/// 2. For each statement:
///    - `ColumnInfo` - Column metadata (SELECT queries only)
///    - `Row` - Zero or more result rows (SELECT queries only)
///    - `StatementCompleted` - Statement finished with row count and type
/// 3. `QueryCompleted` - All statements in the query finished
///
/// Errors can occur at any stage of the response and
#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Request {
    /// For creating, deleting and listing databases in the system.
    CreateDatabase {
        database_name: String,
    },
    DeleteDatabase {
        database_name: String,
    },
    ListDatabases,

    /// For connecting to a given database, thus creating a session, where queries can be sent without
    /// passing database name. Can also be used to switch to a different database while already in a
    /// session, which maintains it.
    Connect {
        database_name: String,
    },

    /// For querying (DDL & DML) a database.
    Query {
        /// If none is provided we use the database name in the session. If there is no session
        /// , meaning this is a one-off query, we respond with an error.
        #[serde(skip_serializing_if = "Option::is_none")]
        database_name: Option<String>,
        sql: String,
    },
}

#[derive(Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Response {
    /// Confirms that the user connected with the database with the given name.
    Connected { database_name: String },

    /// Acknowledges that client sent over a query and confirms that the work on executing it has
    /// started
    Acknowledge,

    /// Contains the metadata of all the columns of the current query's result set. Sent before
    /// sending the rows with the [`Row`] response.
    ColumnInfo {
        // TODO: Change to column metadata type probably,
        column_metadata: Vec<String>,
    },

    /// Contains all the column values of a single row from the current query's result set.
    Row {
        // TODO: Change to record type probably,
        record: Vec<String>,
    },

    /// Lets the client know that all the result for this statement have been sent and that the
    /// server is proceeding onto the next statement. Multiple instances of this may be sent
    /// during the course of a query execution if it contains multiple statements.
    StatementCompleted {
        /// The amount of selected/modified records.
        rows_affected: u32,
        /// For allowing the client to display different messages (e.g. for table drop 'TABLE DROPPED'
        /// and for select '({rows_affected} rows)').
        statement_type: StatementType,
    },

    /// Lets the client know that all the statements (possibly only one) in this query have been
    /// completed and that it can send the next query.
    QueryCompleted,

    /// Sent when an error occurred during any part of the process of handling a request. Contains
    /// the error message and possibly an error code for distinguishing whether it happened because
    /// of an internal issue, a query not following the coSQL grammar or a networking/request problem.
    Error {
        statement_index: Option<u8>,
        message: String,
        error_code: u16,
    },

    /// Confirms that the database with the given name was created
    DatabaseCreated { database_name: String },

    /// Confirms that the database with the given name was deleted
    DatabaseDeleted { database_name: String },

    /// Contains the list of all databases in the system.
    DatabasesListed { database_names: Vec<String> },
}

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
}
