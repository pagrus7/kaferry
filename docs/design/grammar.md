# Kaferry statement grammar

Contributor reference for the parser's accepted structure. The [SQL reference](../reference.md#sql-statements) defines lexical rules, validation, bounds and final options; [usage](../usage.md) shows complete workflows.

The parser accepts the structure below; planning and endpoint initialization enforce the value, scope and identity constraints in the reference. Grouped `OR` is limited to disjoint partition offset ranges.

## EBNF

```ebnf
statement   = [ connections ] insert select [ where ] [ options ] [ ";" ] ;
connections = "WITH" connection { "," connection } ;
connection  = "CONNECTION" alias "AS" "(" property { "," property } ")" ;
property    = string "=" ( string | env ) ;
env         = "ENV" "(" string ")" ;

insert      = "INSERT" "INTO" endpoint [ "WITH" "(" sink_option { "," sink_option } ")" ] ;
select      = "SELECT" "*" "FROM" endpoint [ "WITH" "(" source_option { "," source_option } ")" ] ;
endpoint    = "TOPIC" string                       (* 'topic' or 'topic@alias' *)
            | "ARCHIVE" string ;                   (* archive directory *)
source_option = "GROUP" "=" ( string | "ONE_OFF" )
            | "START" "=" ( "EARLIEST" | "LATEST" )
            | ( "MAX_STALENESS" | "MAX_OUT_OF_ORDER" ) "=" ( interval | "OFF" ) ;
sink_option = "PARTITIONING" "=" ( "PRESERVE" | "KEY" )
            | "ROTATE_AFTER_SIZE" "=" integer [ "k" | "m" | "g" ]
            | "ROTATE_AFTER_AGE" "=" interval ;

where       = "WHERE" ( predicates | branch { "OR" branch } [ "AND" predicates ] ) ;
predicates  = predicate { "AND" predicate } ;
branch      = "(" predicates ")" ;  (* disjoint PARTITION IN/BETWEEN plus offset bounds only *)
predicate   = between | bound | partition | offset ;
offset      = "RECORD_OFFSET" ( compare integer | "<" "LATEST"
            | "BETWEEN" integer "AND" integer ) ;
partition   = "PARTITION" [ "NOT" ] ( "IN" "(" integer { "," integer } ")"
            | "BETWEEN" integer "AND" integer ) ;
between     = "RECORD_TIMESTAMP" "BETWEEN" time "AND" time ;
bound       = "RECORD_TIMESTAMP" compare time ;
compare     = ">=" | ">" | "<" | "<=" ;
time        = instant { ( "+" | "-" ) interval } ;
instant     = "TIMESTAMP" string | "NOW" "(" ")" ;
interval    = "INTERVAL" string unit ;
unit        = "SECOND" | "MINUTE" | "HOUR" | "DAY" ;

options     = "OPTIONS" "(" copy_option { "," copy_option } ")" ;
copy_option = "WORKER_STOP_AFTER_RECORDS" "=" integer
            | "ON_ERROR" "=" ( "FAIL" | "SKIP" )
            | "ON_MISSING_TIMESTAMP" "=" ( "FAIL" | "SKIP" )
            | "ON_REJECTED_TIMESTAMP" "=" ( "FAIL" | "SKIP" ) ;
```
