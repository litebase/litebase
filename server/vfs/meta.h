#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
typedef struct
{
	int file;
	uint64_t pageCount;
	uint64_t pageSize;
	char *path;
} Meta;

Meta *NewMeta(const char *path, int pageSize);

void CloseMeta(Meta *meta);
void MetaAddPage(Meta *meta);
int MetaSetPageCount(Meta *meta, uint64_t pageCount);
uint64_t MetaFileSize(Meta *meta);
