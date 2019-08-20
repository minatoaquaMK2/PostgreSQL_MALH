// query.h ... interface to query scan functions
// part of Multi-attribute Linear-hashed Files
// See query.c for details of Query type and functions

#ifndef QUERY_H
#define QUERY_H 1

typedef struct QueryRep *Query;

#include "reln.h"
#include "tuple.h"

Query startQuery(Reln, char *);
Tuple getNextTuple(Query);
void closeQuery(Query);

#endif
