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

| Type      | Description                                      |
|-----------|--------------------------------------------------|
| `int32`   | 32-bit signed integer                            |
| `int64`   | 64-bit signed integer                            |
| `float32` | 32-bit floating point number                     |
| `float64` | 64-bit floating point number                     |
| `bool`    | Boolean value                                    |
| `string`  | UTF-8 encoded text                               |
| `date`    | Date stored as Unix epoch days (00:00:00 UTC)    |
| `datetime`| Date and time stored as Unix epoch seconds (UTC) |

## Grammar specification

### Conventions
- Keywords are case-insensitive ("Select" is the same as "SELECT" is the same as "select")
- Statements end with semicolons ("SELECT * FROM Users;")
- Number of spaces between tokens doesn't matter (E.g "Select    * FROM Users;" works just fine)
- Strings are enclosed by a single quotation mark ('Text')
- Identifiers must start with a letter and can use only letters, digits and underscores ('_')

### Reserved keywords

SELECT, FROM, WHERE, INSERT, INTO, VALUES, UPDATE, SET, DELETE, TRUE, FALSE

### Elements

#### Identifiers

```
 <identifier> :: <letter> (<letter> | <digit> | '_')*
 <letter> :: 'A'..'Z' | 'a'..'z'
 <digit> :: '0'..'9'
```

#### Literals

```
 <literal> :: <string> | <float> | <int> | <bool>
 <string> ::  "'"character*"'"
 <float> :: <digit>*.<digit><digit>*
 <int> :: <digit><digit>*
 <bool> :: 'TRUE' | 'FALSE'
```
#### Operators
```
<add_op> :: '+' | '-'
<mul_op> :: '*' | '/' | '%'
<unary_op> :: '-' | '+'
<comparison_op> :: '=' | '!=' | '>' | '>=' | '<' | '<='
<logical_op> :: 'AND' | 'OR'
```
#### Expressions
Precedence goes as follows (from highest to lowest):
- primary expressions: function calls, literals, identifiers and parentheses
- unary expressions
- multiplicative expressions 
- additive expressions
- comparison expressions
- logical expressions
```
<expression> :: <logical_expr>
<logical_expr> :: <comparison_expr> (<logical_op> <comparison_expr>)*
<comparison_expr> :: <add_expr> (<comparison_op> <add_expr>)*
<add_expr> :: <mul_expr> (<add_op> <mul_expr>)*
<mul_expr> :: <unary_expr> (<mul_op> <unary_expr>)*
<unary_expr> :: <unary_op> <primary_expr> | <primary_expr>
<primary_expr> :: <function_call> | <literal> | <identifier> | (<expression>)
```

#### Function Calls
```
<function_call> :: <identifier> (<expression_list>)
<expression_list> :: <expression> (',' <expression>)* | ε
```

#### Database specifics

```
 <column_name> :: <identifier>
 <table_name> :: <identifier>
 <value> :: <literal> | <expression>
```

#### Start

```
 <query> :: (<statement>;)*
 <statement> :: <select_stmt> | <insert_stmt> | <update_stmt> | <delete_stmt>
```

#### Where clause
```
 <where_clause> :: WHERE <expression> | ε
```
#### Select
```
 <select_stmt> :: SELECT <column_list> FROM <table_name> <where_clause>
 <column_list_select> :: <column_name>(,<column_name>)* | *
```

#### Insert

```
 <insert_stmt> :: INSERT INTO <table_name> <column_list> VALUES (<values_list>)
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