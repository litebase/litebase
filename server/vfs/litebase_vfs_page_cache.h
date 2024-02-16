#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct LitebaseVFSCachePage
{
	int key;
	void *value;
	struct LitebaseVFSCachePage *next;
	struct LitebaseVFSCachePage *prev;
} LitebaseVFSCachePage;

typedef struct LitebaseVFSCache
{
	int capacity;
	LitebaseVFSCachePage **index;
	int (*Get)(struct LitebaseVFSCache *, int, void *data);
	void (*Put)(struct LitebaseVFSCache *, int, void *);
	void (*Delete)(struct LitebaseVFSCache *, int);
	void (*Flush)(struct LitebaseVFSCache *);
} LitebaseVFSCache;

LitebaseVFSCache *createCache(int capacity);
