#include "./data_range.h"
#include "./log.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
A data range represents a subset of the data in a database. It is used to split
the database into smaller files to allow the database to scale to larger sizes
that typically would not be possible with a single file.
*/
const int DataRangeMaxPages = 1024;

char *getPath(const char *path, int number)
{
	// Get the page number with 10 digits and leading zeros
	char *pageNumber = (char *)malloc(11);
	snprintf(pageNumber, 11, "%010d", number);

	int pathLength = strlen(path);
	int pageNumberLength = strlen(pageNumber);

	// Get name of the file without the extension to use it as a directory
	char *directory = (char *)malloc(pathLength + 1);
	strncpy(directory, path, pathLength + 1);

	// Create the full path
	char *fullPath = (char *)malloc(256);
	snprintf(fullPath, pathLength + pageNumberLength + 2, "%s/%s", directory, pageNumber);

	free(directory);
	free(pageNumber);

	return fullPath;
}

int pageRange(int pageNumber)
{
	return ((pageNumber - 1) / DataRangeMaxPages) + 1;
}

int pageRangeOffset(int pageNumber, int pageSize)
{
	return (pageNumber - 1) % DataRangeMaxPages * pageSize;
}

DataRange *NewDataRange(const char *path, int rangeNumber, int pageSize)
{
	DataRange *dr;

	dr = malloc(sizeof(DataRange));

	dr->number = rangeNumber;
	dr->pageSize = pageSize;
	dr->path = getPath(path, rangeNumber);
	dr->file = open(dr->path, O_CREAT | O_RDWR, 0644);

	if (dr->file == -1)
	{
		fprintf(stderr, "Error opening range file: %s\n", strerror(errno));
		CloseDataRange(dr);

		return NULL;
	}

	return dr;
}

void CloseDataRange(DataRange *dr)
{
	printf("CloseDataRange\n");
	close(dr->file);
	free(dr->path);
	free(dr);
}

int DataRangeReadAt(DataRange *dr, void *buffer, int iAmt, int pageNumber, int *pReadBytes)
{
	int offset = pageRangeOffset(pageNumber, dr->pageSize);

	// Check if the file is NULL
	if (dr->file == -1)
	{
		vfs_log("Error reading data range %d: file is NULL\n", pageNumber);

		return SQLITE_IOERR;
	}

	// Seek to the beginning of the page
	if (lseek(dr->file, offset, SEEK_SET) == -1)
	{
		vfs_log("Error seeking to page %d\n", pageNumber);

		return SQLITE_IOERR_SEEK;
	}

	size_t nRead = read(dr->file, buffer, iAmt);

	// Read the page
	if (nRead < (size_t)iAmt)
	{
		if (nRead == 0)
		{
			// If we hit EOF, zero out the rest of the buffer
			memset((char *)buffer + nRead, 0, iAmt - nRead);

			return SQLITE_IOERR_SHORT_READ;
		}

		return SQLITE_IOERR_READ;
	}

	// Return the number of bytes read when we have a true successful read
	*pReadBytes = (int)nRead;

	return SQLITE_OK;
}

int DataRangeWriteAt(DataRange *dr, const void *buffer, int pageNumber)
{
	int offset = pageRangeOffset(pageNumber, dr->pageSize);

	// Seek to the beginning of the page
	if (lseek(dr->file, offset, SEEK_SET) == -1)
	{
		vfs_log("Error seeking to page %d\n", pageNumber);

		return SQLITE_IOERR_SEEK;
	}

	// Write the page
	if (write(dr->file, buffer, dr->pageSize) == -1)
	{
		vfs_log("Error writing page %d\n", pageNumber);

		return SQLITE_IOERR_WRITE;
	}

	return SQLITE_OK;
}
