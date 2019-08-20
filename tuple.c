// tuple.c ... functions on tuples
// part of Multi-attribute Linear-hashed Files
// Last modified by John Shepherd, July 2019

#include "defs.h"
#include "tuple.h"
#include "reln.h"
#include "hash.h"
#include "chvec.h"
#include "bits.h"
#include "util.h"


// return number of bytes/chars in a tuple

int tupLength(Tuple t)
{
	return strlen(t);
}

// reads/parses next tuple in input

Tuple readTuple(Reln r, FILE *in)
{
	char line[MAXTUPLEN];
	if (fgets(line, MAXTUPLEN-1, in) == NULL)
		return NULL;
	line[strlen(line)-1] = '\0';
	// count fields
	// cheap'n'nasty parsing
	char *c; int nf = 1;
	for (c = line; *c != '\0'; c++)
		if (*c == ',') nf++;
	// invalid tuple
	if (nf != nattrs(r)) return NULL;
	return copyString(line); // needs to be free'd sometime
}

// extract values into an array of strings

void tupleVals(Tuple t, char **vals)
{
	char *c = t, *c0 = t;
	int i = 0;
	for (;;) {
		while (*c != ',' && *c != '\0') c++;
		if (*c == '\0') {
			// end of tuple; add last field to vals
			vals[i++] = copyString(c0);
			break;
		}
		else {
			// end of next field; add to vals
			*c = '\0';
			vals[i++] = copyString(c0);
			*c = ',';
			c++; c0 = c;
		}
	}
}

// release memory used for separate attirubte values

void freeVals(char **vals, int nattrs)
{
	int i;
	for (i = 0; i < nattrs; i++) free(vals[i]);
}

// hash a tuple using the choice vector
// TODO: actually use the choice vector to make the hash

Bits tupleHash(Reln r, Tuple t)
{
    int att,bit;
    ChVecItem *cv = chvec(r);
    Count nvals = nattrs(r);
    Bits hashs[nvals];
    char **vals = malloc(nvals*sizeof(char *));
    tupleVals(t, vals);

    for(int i= 0;i < nvals;i++) {
        hashs[i] = hash_any((unsigned char *)vals[i],strlen(vals[i]));
    }
    Bits hash = 0;
    for(int i = 0;i< MAXCHVEC;i++){
        att = cv[i].att;
        bit = cv[i].bit;
        if(bitIsSet(hashs[att],bit)){
            hash = setBit(hash,i);
        }
    }

    char buf[MAXBITS+1];
    bitsString(hash,buf);
    printf("hash(%s) = %s\n",t,buf);
    for(int i= 0;i < nvals;i++) {
        free(vals[i]);
    }
    free(vals);
	return hash;
}

Bits tupleHashNoPrint(Reln r, Tuple t)
{
    int att,bit;
    ChVecItem *cv = chvec(r);
    Count nvals = nattrs(r);
    Bits hashs[nvals];
    char **vals = malloc(nvals*sizeof(char *));
    tupleVals(t, vals);

    for(int i= 0;i < nvals;i++) {
        hashs[i] = hash_any((unsigned char *)vals[i],strlen(vals[i]));
    }
    Bits hash = 0;
    for(int i = 0;i< MAXCHVEC;i++){
        att = cv[i].att;
        bit = cv[i].bit;
        if(bitIsSet(hashs[att],bit)){
            hash = setBit(hash,i);
        }
    }
    for(int i= 0;i < nvals;i++) {
        free(vals[i]);
    }
    free(vals);
	return hash;
}



// compare two tuples (allowing for "unknown" values)

Bool tupleMatch(Reln r, Tuple t1, Tuple t2)
{
	Count na = nattrs(r);
	char **v1 = malloc(na*sizeof(char *));
	tupleVals(t1, v1);
	char **v2 = malloc(na*sizeof(char *));
	tupleVals(t2, v2);
	Bool match = TRUE;
	int i;
	for (i = 0; i < na; i++) {
		// assumes no real attribute values start with '?'
		if (v1[i][0] == '?' || v2[i][0] == '?') continue;
		if (strcmp(v1[i],v2[i]) == 0) continue;
		match = FALSE;
	}
	freeVals(v1,na); freeVals(v2,na);
	return match;
}

// puts printable version of tuple in user-supplied buffer

void tupleString(Tuple t, char *buf)
{
	strcpy(buf,t);
}
