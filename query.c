 // query.c ... query scan functions
// part of Multi-attribute Linear-hashed Files
// Manage creating and using Query objects

#include "defs.h"
#include "query.h"
#include "reln.h"
#include "hash.h"
#include <stdlib.h>


#include "tuple.h"

// A suggestion ... you can change however you like

struct QueryRep {
    Reln    rel;       // need to remember Relation info
    Bits    known;     // the hash value from MAH
    Bits    unknown;   // the unknown bits from MAH
    PageID  curpage;   // current page in scan
    int     is_ovflow; // are we in the overflow pages?
    Offset  curtup;    // offset of current tuple within page
    Offset  curdata;   // offset of current tuple within page
    char **vals;
    int *unknown_flags;
    //TODO
};
Tuple getTupleInPage(Query q);
// take a query string (e.g. "1234,?,abc,?")
// set up a QueryRep object for the scan

Query startQuery(Reln r, char *q)
{
    int attr = nattrs(r);
    int counter = 0;
    for(int i = 0;i<strlen(q);i++){
        if(q[i]==','){
            counter++;
        }
    }
    if (counter!= attr-1){
        return NULL;
    }
    Query new = malloc(sizeof(struct QueryRep));
    assert(new != NULL);
    new->rel = r;
    Bits unknown = 0;
    Bits known = 0;
    ChVecItem *cv = chvec(r);
    int *unknown_flag = malloc(sizeof(int)*nattrs(r));
    //multi-hash
    Bits hashs[attr];
    Bits hash;

    new->vals = malloc(sizeof(char *)*nattrs(r));
    tupleVals(q,new->vals);
    char cmp_tmp[2] = "?\0";
    for(int i=0;i<attr;i++){
        hash = 0;
        if (strcmp(new->vals[i],cmp_tmp)!=0){
            hash = hash_any((unsigned char *)new->vals[i],strlen(new->vals[i]));
            hashs[i]=hash;
            unknown_flag[i]=0;
        }else{
            unknown_flag[i]=1;
        }
        char buf[MAXBITS+1];
        bitsString(hash,buf);
    }
    new->unknown_flags = unknown_flag;

   
    for(int i = 0;i<MAXCHVEC;i++){
        int att = cv[i].att;
        int bit = cv[i].bit;
        if(unknown_flag[att]==1){
            unknown = setBit(unknown,i);
        }else{
            if(bitIsSet(hashs[att],bit)){
                known = setBit(known,i);
            }
        }
    }
    char buf2[MAXBITS+1];
    bitsString(known,buf2);
    char buf3[MAXBITS+1];
    bitsString(unknown,buf3);
    new->unknown = unknown;
    new->known = known;
    int lower=depth(r);
    Bits pid = getLower(known,lower);
    if(pid < splitp(r)){
        pid = getLower(known,lower+1);
    }
    char buf4[MAXBITS+1];
    bitsString(pid,buf4);

    new->curpage = pid;
    new->curtup = 1;
    new->curdata = 0;
    new->is_ovflow = -1;
    // Partial algorithm:
    // form known bits from known attributes
    // form unknown bits from '?' attributes
    // compute PageID of first page
    //   using known bits and first "unknown" value
    return new;
}

// get next tuple during a scan

Tuple getNextTuple(Query q)
{
    Page cur = getPage(dataFile(q->rel),q->curpage);
    if(q->is_ovflow == -1){
        Tuple result = getTupleInPage(q);
        if (result){
            free(cur);
            return result;
        }
    }
    if (pageOvflow(cur)!=-1&&q->is_ovflow == NO_PAGE){
        q->is_ovflow = pageOvflow(cur);
        q->curtup = 1;
        q->curdata = 0;
    }
    free(cur);
    while(q->is_ovflow != NO_PAGE){
        Tuple result = getTupleInPage(q);
        if (result){
            return result;
        }
        cur = getPage(ovflowFile(q->rel),q->is_ovflow);
        if(pageOvflow(cur)!=-1){
            q->is_ovflow = pageOvflow(cur);
            free(cur);
            q->curtup = 1;
            q->curdata = 0;
            continue;
        } else {
            free(cur);
            break;
        }
    }
    int cur_pid = q->curpage;
    if (cur_pid == npages(q->rel) - 1) {
        return NULL;
    }
    int cur_pid_copy = cur_pid;
    int lower = depth(q->rel);
    Bits pid = getLower(q->known, lower);

    if (pid < splitp(q->rel)) {
        ++lower;
    }

    int filp_unknown = ~q->unknown;
    int tmp = cur_pid & q->unknown;
    tmp = tmp|filp_unknown;
    tmp++;
    tmp = tmp&q->unknown;
    cur_pid = cur_pid & filp_unknown;
    cur_pid = cur_pid |tmp;


    if (cur_pid_copy >= cur_pid||cur_pid > npages(q->rel)-1) {
        return NULL;
    }
    q->curpage = cur_pid;
    q->curdata = 0;
    q->curtup = 1;
    q->is_ovflow = NO_PAGE;

    return getNextTuple(q);
    // Partial algorithm:
    // if (more tuples in current page)
    //    get next matching tuple from current page
    // else if (current page has overflow)
    //    move to overflow page
    //    grab first matching tuple from page
    // else
    //    move to "next" bucket
    //    grab first matching tuple from data page
    // endif
    // if (current page has no matching tuples)
    //    go to next page (try again)
    // endif
    //return NULL;
}

Tuple getTupleInPage(Query q){
    Page cur;
    if (q->is_ovflow == -1){
        cur = getPage(dataFile(q->rel),q->curpage);
    }else{
        cur = getPage(ovflowFile(q->rel),q->is_ovflow);
    }
    int last_tuple = pageNTuples(cur);
    if(q->curtup <= last_tuple) {
        int attr = nattrs(q->rel);
        char **vals = malloc(sizeof(char *) * attr);
        int match;
        Tuple result;
        while (q->curtup <= last_tuple) {
            match = 1;
            result = malloc(sizeof(char *) * (strlen(&pageData(cur)[q->curdata])));
            strcpy(result, &pageData(cur)[q->curdata]);
            tupleVals(result, vals);
            for (int i = 0; i < attr; i++) {
                if (!q->unknown_flags[i]) {
                    if (strcmp(q->vals[i], vals[i]) != 0) {
                        match = 0;
                        break;
                    }
                }
            }
            freeVals(vals,attr);
            q->curdata += (strlen(&pageData(cur)[q->curdata]) + 1);
            q->curtup++;
            if (match) {
                break;
            }
            free(result);
        }
        free(vals);
        if (match == 1) {
            //tupleHash(q->rel,result);
            free(cur);
            return result;

        }
    }
    free(cur);
    return NULL;
}

// clean up a QueryRep object and associated data

void closeQuery(Query q)
{


    freeVals(q->vals,nattrs(q->rel));
    //free(q->rel);
    free(q->vals);
    free(q->unknown_flags);
    free(q);
}
