## CoSQL Query Language Grammar

**CoSQL** is a custom, streamlined subset of SQL designed for simplicity and performance in CoDB. It currently supports basic CRUD operations:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`

> ⚠️ Advanced features such as `JOIN` are not supported at this time

---

## Supported Data Types

CoSQL includes a set of essential data types suitable for building production-grade applications:

| Type       | Description                                      |
| ---------- | ------------------------------------------------ |
| `int32`    | 32-bit signed integer                            |
| `int64`    | 64-bit signed integer                            |
| `float32`  | 32-bit floating point number                     |
| `float64`  | 64-bit floating point number                     |
| `bool`     | Boolean value                                    |
| `string`   | UTF-8 encoded text                               |
| `date`     | Date stored as Unix epoch days (00:00:00 UTC)    |
| `datetime` | Date and time stored as Unix epoch seconds (UTC) |

## Grammar specification

### Conventions

- Keywords are case-insensitive ("Select" is the same as "SELECT" is the same as "select")
- Statements end with semicolons ("SELECT \* FROM Users;")
- Number of spaces between tokens doesn't matter (E.g "Select \* FROM Users;" works just fine)
- Strings are enclosed by a single quotation mark ('Text')
- Identifiers must start with a letter and can use only letters, digits and underscores ('\_')
- When the ORDER BY direction is omitted, ASC (ascending) is used by default.

### Reserved keywords

- SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, RENAME, DROP
- INT32, INT64, FLOAT32, FLOAT64, BOOL, STRING, DATE, DATETIME
- WHERE, FROM, INTO, SET, VALUES, AS
- TABLE, COLUMN, PRIMARY_KEY
- TRUE, FALSE
- ORDER, BY, ASC, DESC, LIMIT, OFFSET

### Identifiers

```
 <identifier> :: <letter> (<letter> | <digit> | '_')*
 <letter> :: 'A'..'Z' | 'a'..'z'
 <digit> :: '0'..'9'
```

### Literals

String can contain the following escape sequences:

- `\\` — Backslash
- `\'` — Single quote
- `\n` — Newline
- `\r` — Carriage return
- `\t` — Horizontal tab

```
 <literal> :: <string> | <float> | <int> | <bool>
 <string> ::  "'"character*"'"
 <float> :: <digit>*.<digit><digit>* | <digit><digit>*.<digit>*
 <int> :: <digit><digit>*
 <bool> :: 'TRUE' | 'FALSE'
```

### Operators

```
<add_op> :: '+' | '-'
<mul_op> :: '*' | '/' | '%'
<unary_op> :: '-' | '+' | '!'
<comparison_op> :: '=' | '!=' | '>' | '>=' | '<' | '<='
<logical_op> :: 'AND' | 'OR'
```

### Expressions

Precedence (from lowest to highest)

- logical OR expressions (`OR`)
- logical AND expressions (`AND`)
- equality expressions (`=`, `!=`)
- comparison expressions (`<`, `<=`, `>`, `>=`)
- additive expressions (`+`, `-`)
- multiplicative expressions (`*`, `/`, `%`)
- unary expressions (unary `+`, unary `-`, `!`)
- primary expressions: function calls, literals, identifiers, and parentheses

```
<expression>       ::= <logical_expr>
<logical_or_expr>  ::= <logical_and_expr> ( "OR" <logical_and_expr> )*
<logical_and_expr> ::= <equality_expr> ( "AND" <equality_expr> )*
<equality_expr>    ::= <comparison_expr> ( <equality_op> <comparison_expr> )*
<comparison_expr>  ::= <add_expr> ( <comparison_op> <add_expr> )*
<add_expr>         ::= <mul_expr> ( <add_op> <mul_expr> )*
<mul_expr>         ::= <unary_expr> ( <mul_op> <unary_expr> )*
<unary_expr>       ::= <unary_op> <primary_expr> | <primary_expr>
<primary_expr>     ::= <function_call> | <literal> | <identifier> | "(" <expression> ")"
```

### Function Calls

```
<function_call> :: <identifier> (<expression_list>)
<expression_list> :: <expression> (',' <expression>)* | ε
```

### Database specifics

```
 <column_name> :: <identifier> <column_alias>
 <column_alias> :: '.' <identifier> | ε
 <table_name> :: <identifier> <table_alias>
 <table_alias> :: AS <identifier> | ε
 <value> :: <literal> | <expression>
 <type> :: INT32 | INT64 | FLOAT32 | FLOAT64 | BOOL | STRING | DATE | DATETIME
```

#### Start

```
 <query> :: (<dml_statement>;)* | (<ddl_statement>;)*
 <dml_statement> :: <select_stmt> | <insert_stmt> | <update_stmt> | <delete_stmt>
 <ddl_statement> :: <create_stmt> | <alter_stmt> | <truncate_stmt> | <drop_stmt>
```

#### Where clause

```
 <where_clause> :: WHERE <expression> | ε
```

#### Select

```
 <select_stmt> :: SELECT <column_list_select> FROM <table_name> <select_clauses>
 <column_list_select> :: <column_name>(,<column_name>)* | *
 <select_clauses> :: <select_where_clause> <order_by_clause> <offset_clause> <limit_clause> 
 <select_where_clause> :: <where_clause> | ε
 <order_by_clause> :: ORDER BY <column_name> <order_direction> | ε
 <order_direction> :: ASC | DESC | ε
 <offset_clause> :: OFFSET <int> | ε
 <limit_clause> :: LIMIT <int> | ε
```

#### Insert

```
 <insert_stmt> :: INSERT INTO <table_name> <column_list_insert> VALUES (<values_list>)
 <column_list_insert> :: (<column_name>(,<column_name>)*) | ε
 <values_list> :: (<value>(,<value>)*)
```

#### Update

```
 <update_stmt> :: UPDATE <table_name> SET <column_setters> <where_clause>
 <column_setters> :: <column_setter>(,<column_setter>)*
 <column_setter> :: <column_name>=<expression>
```

#### Delete

```
 <delete_stmt> :: DELETE FROM <table_name> <where_clause>
```

#### Create Table

```
 <create_stmt> :: CREATE TABLE <table_name> (<create_args>)
 <create_args> :: <create_arg>(,<create_arg>)*
 <create_arg> :: <column_name> <type> <column_addons>
 <column_addons> :: PRIMARY_KEY | ε
```

> TODO: In the future we should add more addons such as FOREIGN_KEY

#### Alter Table

```
 <alter_stmt> :: ALTER TABLE <table_name> <alter_action>
 <alter_action> :: <add_alter_action> | <rename_alter_action> | <drop_alter_action>
 <add_alter_action> :: ADD <column_name> <type>
 <rename_alter_action> :: RENAME <rename_target>
 <rename_target> :: COLUMN <column_name> TO <column_name> | TABLE TO <table_name>
 <drop_alter_action> :: DROP COLUMN <column_name>
```

#### Truncate Table

```
 <truncate_stmt> :: TRUNCATE TABLE <table_name>
```

#### Drop Table

```
 <drop_stmt> :: DROP TABLE <table_name>
```
