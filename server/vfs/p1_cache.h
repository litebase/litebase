#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct CachePage
{
	int key;
	void *value;
	struct CachePage *next;
	struct CachePage *prev;
} CachePage;

typedef struct P1Cache
{
	int capacity;
	char *id;
	CachePage **index;
	int (*Get)(struct P1Cache *, int, void *data);
	void (*Put)(struct P1Cache *, int, void *);
	void (*Delete)(struct P1Cache *, int);
	void (*Flush)(struct P1Cache *);
} P1Cache;

P1Cache *createCache(char *id, int capacity);

void P1CacheDelete(P1Cache *cache, int pageNumber);
void P1CacheFlush(P1Cache *cache);

extern int goSpillCachePage(char *cacheId, int pageNumber, void *data);
