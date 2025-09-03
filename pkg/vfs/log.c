#include "log.h"
#include <stdarg.h>
#include <stdio.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/time.h>
#endif

#define LOG_ENABLED 1

void vfs_log(const char *format, ...)
{
	if (!LOG_ENABLED)
	{
		return;
	}

	// Get the current time
	time_t now = time(NULL);
	struct tm *local_time = localtime(&now);

	// Print the log message
	printf("[%02d:%02d:%02d] [VFS LOG] ", local_time->tm_hour, local_time->tm_min, local_time->tm_sec);

	// Use the variable argument list to print the message
	va_list args;
	va_start(args, format);
	vprintf(format, args);
	va_end(args);

	printf("\n");
}

int vfs_log_start()
{
	if (!LOG_ENABLED)
	{
		return 0;
	}

#ifdef _WIN32
	// Windows implementation using QueryPerformanceCounter
	LARGE_INTEGER frequency, counter;
	QueryPerformanceFrequency(&frequency);
	QueryPerformanceCounter(&counter);
	return (int)(counter.QuadPart * 1000000000LL / frequency.QuadPart);
#else
	// Unix/Linux implementation using clock_gettime
	struct timespec start;
	clock_gettime(CLOCK_MONOTONIC, &start);
	return start.tv_sec * 1000000000L + start.tv_nsec;
#endif
}

void vfs_log_end(int starttime, const char *description, ...)
{
	if (!LOG_ENABLED)
	{
		return;
	}

	int endtime;

#ifdef _WIN32
	// Windows implementation using QueryPerformanceCounter
	LARGE_INTEGER frequency, counter;
	QueryPerformanceFrequency(&frequency);
	QueryPerformanceCounter(&counter);
	endtime = (int)(counter.QuadPart * 1000000000LL / frequency.QuadPart);
#else
	// Unix/Linux implementation using clock_gettime
	struct timespec end;
	clock_gettime(CLOCK_MONOTONIC, &end);
	endtime = end.tv_sec * 1000000000L + end.tv_nsec;
#endif

	// Print the log message
	printf("[%s] - took %d nanoseconds\n", description, endtime - starttime);
}
