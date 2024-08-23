#include "./meta.h"

/*
The _METADATA file contains information about the database such as the number of
pages so that a SQLite file size can be determined.

The page count is stored as a 64-bit integer at offset 0.
*/
static char *getPath(const char *path)
{
	// Determine the required buffer size
	int size = snprintf(NULL, 0, "%s/_METADATA", path) + 1;

	// Allocate memory
	char *metaPath = (char *)malloc(size);

	if (metaPath == NULL)
	{
		// Handle memory allocation failure
		return NULL;
	}

	// Format the string
	sprintf(metaPath, "%s/_METADATA", path);

	return metaPath;
}

static int readMeta(Meta *meta)
{
	// Seek to the beginning of the file
	if (lseek(meta->file, 0, SEEK_SET) == -1)
	{
		// Handle seek error
		fprintf(stderr, "Error seeking metadata: %s\n", strerror(errno));
		fprintf(stderr, "PATH: %s\n", meta->path);
		fprintf(stderr, "FD: %d\n", meta->file);

		return 1;
	}

	// Read the metadata from the file
	if (read(meta->file, &meta->pageCount, sizeof(meta->pageCount)) == -1)
	{
		// Handle read error
		fprintf(stderr, "Error reading metadata: %s\n", strerror(errno));

		return 1;
	}

	return 0;
}

int saveMeta(Meta *meta)
{
	// Seek to the beginning of the file
	if (lseek(meta->file, 0, SEEK_SET) == -1)
	{
		// Handle seek error
		fprintf(stderr, "Error seeking metadata: %s\n", strerror(errno));

		return 1;
	}

	// Write the metadata to the file
	if (write(meta->file, &meta->pageCount, sizeof(meta->pageCount)) == -1)
	{
		// Handle write error
		fprintf(stderr, "Error writing metadata: %s\n", strerror(errno));

		return 1;
	}

	return 0;
}

Meta *NewMeta(const char *path, int pageSize)
{
	Meta *meta;

	meta = malloc(sizeof(Meta));
	meta->path = getPath(path);
	meta->pageSize = pageSize;
	meta->pageCount = 0;

	meta->file = open(meta->path, O_CREAT | O_RDWR, 0644);

	if (meta->file == -1)
	{
		if (errno == ENOENT)
		{
			// If the directory does not exist, create it then try to open the file again
			if (stat(path, NULL) == -1)
			{
				if (mkdir(path, 0755) == -1)
				{
					fprintf(stderr, "Error creating directory: %s\n", strerror(errno));
				}
				else
				{
					meta->file = open(meta->path, O_CREAT | O_RDWR, 0644);
				}
			}
		}
		else
		{
			fprintf(stderr, "Error opening meta file: %s\n", strerror(errno));

			CloseMeta(meta);

			return NULL;
		}
	}

	if (meta->file == -1)
	{
		fprintf(stderr, "Error opening meta file: %s\n", strerror(errno));

		CloseMeta(meta);

		return NULL;
	}

	readMeta(meta);

	return meta;
}

void CloseMeta(Meta *meta)
{
	close(meta->file);
	free(meta->path);
	free(meta);
}

void MetaAddPage(Meta *meta)
{
	int rc;

	meta->pageCount++;

	rc = saveMeta(meta);

	if (rc != 0)
	{
		fprintf(stderr, "Error saving metadata\n");
		meta->pageCount--;
	}
}

int MetaSetPageCount(Meta *meta, uint64_t pageCount)
{
	int rc;

	if (pageCount < 0)
	{
		fprintf(stderr, "[MetaSetPageCount] Cannot set page count to a value less than the current page count\n");

		return 1;
	}

	meta->pageCount = pageCount;

	rc = saveMeta(meta);

	if (rc != 0)
	{
		fprintf(stderr, "Error saving metadata\n");

		return 1;
	}

	return 0;
}

uint64_t MetaFileSize(Meta *meta)
{
	return meta->pageCount * meta->pageSize;
}
