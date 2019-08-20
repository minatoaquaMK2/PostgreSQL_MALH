// reln.c ... functions on Relations
// part of Multi-attribute Linear-hashed Files

#include "defs.h"
#include "reln.h"
#include "page.h"
#include "tuple.h"
#include "chvec.h"
#include "bits.h"
#include "hash.h"
#include <math.h>

#define HEADERSIZE (3*sizeof(Count)+sizeof(Offset))

struct RelnRep {
    Count  nattrs; // number of attributes
    Count  depth;  // depth of main data file
    Offset sp;     // split pointer
    Count  npages; // number of main data pages
    Count  ntups;  // total number of tuples
    ChVec  cv;     // choice vector
    char   mode;   // open for read/write
    FILE  *info;   // handle on info file
    FILE  *data;   // handle on data file
    FILE  *ovflow; // handle on ovflow file
};

// create a new relation (three files)

Status newRelation(char *name, Count nattrs, Count npages, Count d, char *cv)
{
    char fname[MAXFILENAME];
    Reln r = malloc(sizeof(struct RelnRep));
    r->nattrs = nattrs; r->depth = d; r->sp = 0;
    r->npages = npages; r->ntups = 0; r->mode = 'w';
    assert(r != NULL);
    if (parseChVec(r, cv, r->cv) != OK) return ~OK;
    sprintf(fname,"%s.info",name);
    r->info = fopen(fname,"w");
    assert(r->info != NULL);
    sprintf(fname,"%s.data",name);
    r->data = fopen(fname,"w");
    assert(r->data != NULL);
    sprintf(fname,"%s.ovflow",name);
    r->ovflow = fopen(fname,"w");
    assert(r->ovflow != NULL);
    int i;
    for (i = 0; i < npages; i++) addPage(r->data);
    closeRelation(r);
    return 0;
}

// check whether a relation already exists

Bool existsRelation(char *name)
{
    char fname[MAXFILENAME];
    sprintf(fname,"%s.info",name);
    FILE *f = fopen(fname,"r");
    if (f == NULL)
        return FALSE;
    else {
        fclose(f);
        return TRUE;
    }
}

// set up a relation descriptor from relation name
// open files, reads information from rel.info

Reln openRelation(char *name, char *mode)
{
    Reln r;
    r = malloc(sizeof(struct RelnRep));
    assert(r != NULL);
    char fname[MAXFILENAME];
    sprintf(fname,"%s.info",name);
    r->info = fopen(fname,mode);
    assert(r->info != NULL);
    sprintf(fname,"%s.data",name);
    r->data = fopen(fname,mode);
    assert(r->data != NULL);
    sprintf(fname,"%s.ovflow",name);
    r->ovflow = fopen(fname,mode);
    assert(r->ovflow != NULL);
    // Naughty: assumes Count and Offset are the same size
    int n = fread(r, sizeof(Count), 5, r->info);
    assert(n == 5);
    n = fread(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
    assert(n == MAXCHVEC);
    r->mode = (mode[0] == 'w' || mode[1] =='+') ? 'w' : 'r';
    return r;
}

// release files and descriptor for an open relation
// copy latest information to .info file

void closeRelation(Reln r)
{
    // make sure updated global data is put in info
    // Naughty: assumes Count and Offset are the same size
    if (r->mode == 'w') {
        fseek(r->info, 0, SEEK_SET);
        // write out core relation info (#attr,#pages,d,sp)
        int n = fwrite(r, sizeof(Count), 5, r->info);
        assert(n == 5);
        // write out choice vector
        n = fwrite(r->cv, sizeof(ChVecItem), MAXCHVEC, r->info);
        assert(n == MAXCHVEC);
    }
    fclose(r->info);
    fclose(r->data);
    fclose(r->ovflow);
    free(r);
}

// insert a new tuple into a relation
// returns index of bucket where inserted
// - index always refers to a primary data page
// - the actual insertion page may be either a data page or an overflow page
// returns NO_PAGE if insert fails completely
// TODO: include splitting and file expansion
int needSplit(Reln r){
    int attr = nattrs(r);
    int tups = r->ntups;
    int tmp = floor(1024/(attr*10));
    if(tups%tmp==0&&tups!=0){
        return 1;
    } else {
        return 0;
    }
}


int tupsInPageAndOV(Reln r,int pid){
    int result = 0;

    Page cur = getPage(dataFile(r),pid);
    result += pageNTuples(cur);
    int ov = pageOvflow(cur);
    free(cur);
    while(ov != -1){

        cur = getPage(ovflowFile(r),ov);
        result += pageNTuples(cur);
        ov = pageOvflow(cur);
        free(cur);
    }
    return result;
}

char **allTups(Reln r){
    char **tups = malloc(sizeof(char *)*tupsInPageAndOV(r,r->sp));
    Page sp = getPage(dataFile(r),r->sp);
    Offset cur = 1;
    Offset cur_data = 0;
    int counter = 0;
    while(cur <= pageNTuples(sp)){
        int tup_len = strlen(&pageData(sp)[cur_data]);
        char *tup = malloc(sizeof(char)*(tup_len+1));
        strcpy(tup,&pageData(sp)[cur_data]);
        tup[tup_len] = '\0';
        tups[counter] = tup;
        cur_data+=tup_len+1;
        cur++;
        counter++;
    }
    int ov = pageOvflow(sp);
    free(sp);

    while(ov!=-1){
        cur = 1;
        cur_data = 0;
        Page cur_page = getPage(ovflowFile(r),ov);
        while(cur <= pageNTuples(cur_page)){
            int tup_len = strlen(&pageData(cur_page)[cur_data]);
            char *tup = malloc(sizeof(char)*(tup_len+1));
            strcpy(tup,&pageData(cur_page)[cur_data]);
            tup[tup_len] = '\0';
            tups[counter] = tup;
            cur_data+=tup_len+1;
            cur++;
            counter++;
        }
        ov = pageOvflow(cur_page);
        free(cur_page);
    }
    return tups;
}

void cleanPage(Reln r,int pid){
    //clean page and ov

    Page cur = getPage(dataFile(r),pid);

    Offset ov = pageOvflow(cur);
    pageClean(cur);
    putPage(dataFile(r),pid,cur);
    while (ov != NO_PAGE){

        cur = getPage(ovflowFile(r),ov);
        int next = pageOvflow(cur);
        pageClean(cur);
        putPage(ovflowFile(r),ov,cur);
        ov = next;
    }
}

PageID addToRelation(Reln r, Tuple t)
{
    if (needSplit(r)){

        int total_tups = tupsInPageAndOV(r,r->sp);
        char **tups = allTups(r);
        //Tuple tup;
        int n_pid = addPage(r->data);
        r->npages++;
        cleanPage(r,r->sp);
        //cleanPage;
        for(int i= 0;i<total_tups;i++){
            Bits hash= tupleHashNoPrint(r,tups[i]);
            Bits lower = getLower(hash,r->depth+1);
            if(bitIsSet(lower,r->depth)){
                //insert to new page
                Page pg = getPage(r->data,n_pid);
                if (addToPage(pg,tups[i]) == OK) {
                    putPage(r->data,n_pid,pg);
                    continue;
                }
                // primary data page full
                if (pageOvflow(pg) == NO_PAGE) {
                    // add first overflow page in chain
                    PageID newp = addPage(r->ovflow);
                    pageSetOvflow(pg,newp);
                    putPage(r->data,n_pid,pg);
                    Page newpg = getPage(r->ovflow,newp);
                    // can't add to a new page; we have a problem
                    if (addToPage(newpg,tups[i]) != OK) return NO_PAGE;
                    putPage(r->ovflow,newp,newpg);
                    continue;
                }
                else {
                    // scan overflow chain until we find space
                    // worst case: add new ovflow page at end of chain
                    Page ovpg, prevpg = NULL;
                    PageID ovp, prevp = NO_PAGE;
                    ovp = pageOvflow(pg);
                    free(pg);
                    int ok = 0;
                    while (ovp != NO_PAGE) {
                        ovpg = getPage(r->ovflow, ovp);
                        if (addToPage(ovpg,tups[i]) != OK) {
                            prevp = ovp;
                            if(prevpg){
                                free(prevpg);
                            }
                            prevpg = ovpg;
                            ovp = pageOvflow(ovpg);
                        }
                        else {
                            if (prevpg != NULL) free(prevpg);
                            putPage(r->ovflow,ovp,ovpg);
                            ok = 1;
                            break;
                        }
                    }
                    // all overflow pages are full; add another to chain
                    // at this point, there *must* be a prevpg
                    if(ok==0){
                        assert(prevpg != NULL);
                        // make new ovflow page

                        PageID newp = addPage(r->ovflow);
                        //printf("b new:%d\n",newp);

                        // insert tuple into new page
                        Page newpg = getPage(r->ovflow,newp);
                        if (addToPage(newpg,t) != OK) return NO_PAGE;
                        putPage(r->ovflow,newp,newpg);
                        // link to existing overflow chain
                        pageSetOvflow(prevpg,newp);
                        putPage(r->ovflow,prevp,prevpg);
                    }

                }
            }else{
                //insert to sp page
                Page pg = getPage(r->data,r->sp);
                if (addToPage(pg,tups[i]) == OK) {
                    putPage(r->data,r->sp,pg);
                    continue;
                }
                if (pageOvflow(pg) == NO_PAGE) {
                    // add first overflow page in chain
                    PageID newp = addPage(r->ovflow);
                    pageSetOvflow(pg,newp);
                    putPage(r->data,r->sp,pg);
                    Page newpg = getPage(r->ovflow,newp);
                    // can't add to a new page; we have a problem
                    if (addToPage(newpg,tups[i]) != OK) return NO_PAGE;
                    putPage(r->ovflow,newp,newpg);
                    continue;

                }
                else {
                    // scan overflow chain until we find space
                    // worst case: add new ovflow page at end of chain
                    Page ovpg, prevpg = NULL;
                    PageID ovp, prevp = NO_PAGE;
                    ovp = pageOvflow(pg);
                    free(pg);
                    int ok = 0;
                    while (ovp != NO_PAGE) {
                        ovpg = getPage(r->ovflow, ovp);
                        if (addToPage(ovpg, tups[i]) != OK) {
                            prevp = ovp;
                            if(prevpg){
                                free(prevpg);
                            }
                            prevpg = ovpg;
                            ovp = pageOvflow(ovpg);
                        } else {
                            if (prevpg != NULL) free(prevpg);
                            putPage(r->ovflow, ovp, ovpg);
                            ok = 1;
                            break;
                        }
                    }
                    // all overflow pages are full; add another to chain
                    // at this point, there *must* be a prevpg
                    //**********************************************
                    if (ok !=1){
                        assert(prevpg != NULL);

                        PageID newp = addPage(r->ovflow);
                        Page newpg = getPage(r->ovflow,newp);
                        if (addToPage(newpg,t) != OK) return NO_PAGE;
                        putPage(r->ovflow,newp,newpg);
                        pageSetOvflow(prevpg,newp);
                        putPage(r->ovflow,prevp,prevpg);

                        continue;
                    }

                }
            }
        }

        r->sp++;
        if(r->sp == pow(2,r->depth)){
            r->sp=0;
            r->depth++;
        }
        for(int i= 0;i<total_tups;i++){
            free(tups[i]);
        }
        free(tups);
    }

    Bits h, p;
    h = tupleHash(r,t);
    if (r->depth == 0)
        p = 1;
    else {
        p = getLower(h, r->depth);
        if (p < r->sp) p = getLower(h, r->depth+1);
    }
    // bitsString(h,buf); printf("hash = %s\n",buf);
    // bitsString(p,buf); printf("page = %s\n",buf);
    Page pg = getPage(r->data,p);
    if (addToPage(pg,t) == OK) {
        putPage(r->data,p,pg);
        r->ntups++;
        return p;
    }
    // primary data page full
    if (pageOvflow(pg) == NO_PAGE) {
        // add first overflow page in chain
        PageID newp = addPage(r->ovflow);
        //printf("new:%d\n",newp);
        //printf("468\n");

        pageSetOvflow(pg,newp);
        putPage(r->data,p,pg);
        Page newpg = getPage(r->ovflow,newp);
        // can't add to a new page; we have a problem
        if (addToPage(newpg,t) != OK) return NO_PAGE;
        putPage(r->ovflow,newp,newpg);
        r->ntups++;
        return p;
    } else {
        // scan overflow chain until we find space
        // worst case: add new ovflow page at end of chain
        Page ovpg, prevpg = NULL;
        PageID ovp, prevp = NO_PAGE;
        ovp = pageOvflow(pg);
        free(pg);
        while (ovp != NO_PAGE) {
            ovpg = getPage(r->ovflow, ovp);
            if (addToPage(ovpg,t) != OK) {
                prevp = ovp;
                if(prevpg){
                    free(prevpg);
                }
                prevpg = ovpg;
                ovp = pageOvflow(ovpg);
                //free(ovpg);
            }
            else {
                if (prevpg != NULL) free(prevpg);
                putPage(r->ovflow,ovp,ovpg);
                r->ntups++;
                return p;
            }
        }
        // all overflow pages are full; add another to chain
        // at this point, there *must* be a prevpg
        assert(prevpg != NULL);
        // make new ovflow page
        PageID newp = addPage(r->ovflow);
        //printf("new:%d\n",newp);
        //printf("505\n");

        // insert tuple into new page
        Page newpg = getPage(r->ovflow,newp);
        if (addToPage(newpg,t) != OK) return NO_PAGE;
        putPage(r->ovflow,newp,newpg);
        // link to existing overflow chain
        pageSetOvflow(prevpg,newp);
        putPage(r->ovflow,prevp,prevpg);
        r->ntups++;
        return p;
    }
    return NO_PAGE;
}




// external interfaces for Reln data

FILE *dataFile(Reln r) { return r->data; }
FILE *ovflowFile(Reln r) { return r->ovflow; }
Count nattrs(Reln r) { return r->nattrs; }
Count npages(Reln r) { return r->npages; }
Count ntuples(Reln r) { return r->ntups; }
Count depth(Reln r)  { return r->depth; }
Count splitp(Reln r) { return r->sp; }
ChVecItem *chvec(Reln r)  { return r->cv; }


// displays info about open Reln

void relationStats(Reln r)
{
    printf("Global Info:\n");
    printf("#attrs:%d  #pages:%d  #tuples:%d  d:%d  sp:%d\n",
           r->nattrs, r->npages, r->ntups, r->depth, r->sp);
    printf("Choice vector\n");
    printChVec(r->cv);
    printf("Bucket Info:\n");
    printf("%-4s %s\n","#","Info on pages in bucket");
    printf("%-4s %s\n","","(pageID,#tuples,freebytes,ovflow)");
    for (Offset pid = 0; pid < r->npages; pid++) {
        printf("[%2d]  ", pid);
        Page p = getPage(r->data, pid);
        Count ntups = pageNTuples(p);
        Count space = pageFreeSpace(p);
        Offset ovid = pageOvflow(p);
        printf("(d%d,%d,%d,%d)", pid, ntups, space, ovid);
        free(p);
        while (ovid != NO_PAGE) {
            Offset curid = ovid;
            p = getPage(r->ovflow, ovid);
            ntups = pageNTuples(p);
            space = pageFreeSpace(p);
            ovid = pageOvflow(p);
            printf(" -> (ov%d,%d,%d,%d)", curid, ntups, space, ovid);
            free(p);
        }
        putchar('\n');
    }
}
