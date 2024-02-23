#include "./p1_cache.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Hash function
int hash(int key, int capacity)
{
	return key % capacity;
}

int Get(P1Cache *cache, int pageNumber, void *data)
{
	int index = hash(pageNumber, cache->capacity);

	if (cache->index[index] == NULL)
	{
		return -1;
	}

	CachePage *entry = cache->index[index];

	while (entry != NULL)
	{
		if (entry->key == pageNumber && entry->value != NULL)
		{
			// Save the next node
			struct CachePage *nextNode = entry->next;

			// If this entry is not already the head of the list
			if (cache->index[index] != entry)
			{
				// Remove entry from its current position
				if (entry->prev)
				{
					entry->prev->next = entry->next;
				}

				if (entry->next)
				{
					entry->next->prev = entry->prev;
				}

				// Insert entry at the head of the list
				entry->next = cache->index[index];
				entry->prev = NULL;

				if (cache->index[index])
				{
					cache->index[index]->prev = entry;
				}

				cache->index[index] = entry;
			}

			memcpy(data, entry->value, 4096);

			return 0;
		}

		entry = entry->next;
	}

	return -1;
}

void Put(P1Cache *cache, int pageNumber, void *data)
{
	int index = hash(pageNumber, cache->capacity);

	if (cache->index[index] != NULL)
	{
		// Get the size of the linked list
		int size = 0;
		CachePage *entry = cache->index[index];
		CachePage *tail;

		while (entry != NULL)
		{

			size++;
			tail = entry;
			entry = entry->next;
		}

		// If the size of the linked list is greater than the capacity
		if (size >= cache->capacity)
		{

			if (tail->prev)
			{
				tail->prev->next = NULL;
			}
			else
			{
				cache->index[index] = NULL;
			}

			// Spill the pages to the P2 cache
			goSpillCachePage(cache->id, tail->key, tail->value);

			// Free the tail from memory
			free(tail->value);
			free(tail);
		}
	}

	CachePage *new_entry = malloc(sizeof(CachePage));
	new_entry->key = pageNumber;
	new_entry->next = cache->index[index];

	if (new_entry->next != NULL)
	{
		new_entry->next->prev = new_entry;
	}

	cache->index[index] = new_entry;

	cache->index[index]->value = malloc(4096);
	memcpy(cache->index[index]->value, data, 4096);
}

void P1CacheDelete(P1Cache *cache, int pageNumber)
{
	int index = hash(pageNumber, cache->capacity);
	CachePage *current = cache->index[index];

	while (current != NULL)
	{
		if (current->key == pageNumber)
		{
			// Update the next pointer of the previous node
			if (current->prev != NULL)
			{
				current->prev->next = current->next;
			}

			// Update the prev pointer of the next node
			if (current->next != NULL)
			{
				current->next->prev = current->prev;
			}

			// If the node to be deleted is the head of the list
			if (current == cache->index[index])
			{
				cache->index[index] = current->next;

				// If the list is not empty after deletion
				if (cache->index[index] != NULL)
				{
					cache->index[index]->prev = NULL;
				}
			}

			free(current->value);
			free(current);

			return;
		}

		current = current->next;
	}
}

void P1CacheFlush(P1Cache *cache)
{
	// Remove all the pages from the index and the data array
	for (int i = 0; i < cache->capacity; ++i)
	{
		CachePage *current = cache->index[i];

		while (current != NULL)
		{
			CachePage *temp = current;
			current = current->next;

			free(temp->value);
			free(temp);
		}

		cache->index[i] = NULL;
	}
}

P1Cache *createCache(char *id, int capacity)
{
	P1Cache *cache = malloc(sizeof(P1Cache));
	cache->id = id;
	cache->capacity = sqrt(capacity);
	cache->index = malloc(sizeof(CachePage *) * capacity);
	cache->Get = Get;
	cache->Put = Put;
	cache->Delete = P1CacheDelete;
	cache->Flush = P1CacheFlush;

	for (int i = 0; i < capacity; ++i)
	{
		cache->index[i] = NULL;
	}

	return cache;
}

void destroyCache(P1Cache *cache)
{
	// Free each index
	cache->Flush(cache);

	// Free the index array and the cache itself
	free(cache->index);
	free(cache);
}
