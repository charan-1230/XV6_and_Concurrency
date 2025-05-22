#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_FILENAME_LEN 128
#define MAX_TIMESTAMP_LEN 20
#define THRESHOLD 42
#define MAX_THREADS 4

typedef struct
{
    char name[MAX_FILENAME_LEN];
    int id;
    char timestamp[MAX_TIMESTAMP_LEN];
    int timehash;
    int nhash;
} File;

File *files; // Original array of files
File *sorted_files;
int file_count; // Total number of files
int max_id;
int min_id;
int max_time;
int min_time;
int max_nhash;
int min_nhash;

typedef struct
{
    int thread_index;
    int *local_counts; // Local count array for each thread
    int start_index;
    int end_index;
    int sort_flag;
} CountSortArgs;

int namehash(char *str)
{
    int hash = 0;
    int index;
    for (int i = 0; i < strlen(str); i++)
    {
        if (str[i] == '.')
        {
            index = i;
            break;
        }
    }
    for (int i = 0; i < index; i++)
    {
        hash = hash * 26 + (str[i] - 'a' + 1);
    }
    // printf("%d->\n",hash);
    return hash;
}

void *count_sort_thread(void *args)
{
    CountSortArgs *count_args = (CountSortArgs *)args;
    int *local_counts = count_args->local_counts;

    for (int i = count_args->start_index; i < count_args->end_index; i++)
    {
        if (count_args->sort_flag == 0)
        {
            local_counts[files[i].id - min_id]++;
        }
        else if (count_args->sort_flag == 1)
        {
            local_counts[files[i].nhash - min_nhash]++;
        }
        else
        {
            local_counts[files[i].timehash - min_time]++;
        }
    }

    return NULL;
}

void parallel_count_sort(int flag)
{
    int size;
    if (flag == 0)
    {
        size = max_id - min_id + 1;
    }
    else if (flag == 1)
    {
        size = max_nhash - min_nhash + 1;
    }
    else
    {
        size = max_time - min_time + 1;
    }
    int *count_array = (int *)calloc(size, sizeof(int)); // Shared counting array for all IDs
    pthread_t threads[MAX_THREADS];
    CountSortArgs thread_args[MAX_THREADS];

    for (int i = 0; i < MAX_THREADS; i++)
    {
        thread_args[i].local_counts = (int *)calloc(size, sizeof(int));
        thread_args[i].thread_index = i;
        thread_args[i].start_index = (i * file_count) / MAX_THREADS;
        thread_args[i].end_index = ((i + 1) * file_count) / MAX_THREADS;
        thread_args[i].sort_flag = flag;
        pthread_create(&threads[i], NULL, count_sort_thread, &thread_args[i]);
    }

    for (int i = 0; i < MAX_THREADS; i++)
    {
        pthread_join(threads[i], NULL);
    }

    // local counts are included into the global count array
    for (int i = 0; i < MAX_THREADS; i++)
    {
        for (int j = 0; j < size; j++)
        {
            count_array[j] += thread_args[i].local_counts[j];
        }
        free(thread_args[i].local_counts);
    }

    for (int i = 1; i < size; i++)
    {
        count_array[i] += count_array[i - 1];
    }

    int pos;
    for (int i = file_count - 1; i >= 0; i--)
    {
        if (flag == 0)
        {
            pos = --count_array[files[i].id - min_id];
        }
        else if (flag == 1)
        {
            pos = --count_array[files[i].nhash - min_nhash];
        }
        else
        {
            pos = --count_array[files[i].timehash - min_time];
        }
        sorted_files[pos] = files[i];
    }

    free(count_array);
}

void distributed_count_sort(const char *sortBy)
{
    if (strcmp(sortBy, "ID") == 0)
    {
        parallel_count_sort(0);
    }
    else if (strcmp(sortBy, "Name") == 0)
    {
        parallel_count_sort(1);
    }
    else
    {
        parallel_count_sort(2);
    }
}

/////////////  merge sort //////////////

typedef struct
{
    File *files;
    int start;
    int end;
    char *sortBy;
} ThreadArgs;

// Function to merge two sorted subarrays
void merge(File *arr, int left, int mid, int right, const char *sortBy)
{
    int i, j, k;
    int n1 = mid - left + 1;
    int n2 = right - mid;

    File *L = (File *)malloc(n1 * sizeof(File));
    File *R = (File *)malloc(n2 * sizeof(File));

    for (i = 0; i < n1; i++)
        L[i] = arr[left + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[mid + 1 + j];

    i = 0;
    j = 0;
    k = left;
    while (i < n1 && j < n2)
    {
        if (strcmp(sortBy, "ID") == 0)
        {
            if (L[i].id <= R[j].id)
            {
                arr[k] = L[i];
                i++;
            }
            else
            {
                arr[k] = R[j];
                j++;
            }
        }
        else if (strcmp(sortBy, "Name") == 0)
        {
            if (strcmp(L[i].name, R[j].name) <= 0)
            {
                arr[k] = L[i];
                i++;
            }
            else
            {
                arr[k] = R[j];
                j++;
            }
        }
        else
        { // Timestamp
            if (strcmp(L[i].timestamp, R[j].timestamp) <= 0)
            {
                arr[k] = L[i];
                i++;
            }
            else
            {
                arr[k] = R[j];
                j++;
            }
        }
        k++;
    }

    while (i < n1)
    {
        arr[k] = L[i];
        i++;
        k++;
    }

    while (j < n2)
    {
        arr[k] = R[j];
        j++;
        k++;
    }

    free(L);
    free(R);
}

// Function to perform merge sort on a portion of the array
void mergeSort(File *arr, int left, int right, const char *sortBy)
{
    if (left < right)
    {
        int mid = left + (right - left) / 2;

        mergeSort(arr, left, mid, sortBy);
        mergeSort(arr, mid + 1, right, sortBy);

        merge(arr, left, mid, right, sortBy);
    }
}

// Thread function for parallel merge sort
void *threadMergeSort(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    mergeSort(args->files, args->start, args->end, args->sortBy);
    return NULL;
}

// Function to perform parallel merge sort
void parallelMergeSort(File *files, int n, const char *sortBy)
{
    pthread_t threads[MAX_THREADS];
    ThreadArgs args[MAX_THREADS];
    int chunk_size = n / MAX_THREADS;

    for (int i = 0; i < MAX_THREADS; i++)
    {
        args[i].files = files;
        args[i].start = i * chunk_size;
        if (i == MAX_THREADS - 1)
        {
            args[i].end = n - 1;
        }
        else
        {
            args[i].end = (i + 1) * chunk_size - 1;
        }

        args[i].sortBy = sortBy;
        pthread_create(&threads[i], NULL, threadMergeSort, &args[i]);
    }

    for (int i = 0; i < MAX_THREADS; i++)
    {
        pthread_join(threads[i], NULL);
    }

    // Merge the sorted subarrays
    for (int size = chunk_size; size < n; size = 2 * size)
    {
        for (int left = 0; left < n - 1; left += 2 * size)
        {
            int mid = left + size - 1;
            int right;
            if (left + 2 * size - 1 < n - 1)
            {
                right = left + 2 * size - 1;
            }
            else
            {
                right = n - 1;
            }

            merge(files, left, mid, right, sortBy);
        }
    }
}

/////////////  merge sort ////////////////////
int isLeapYear(int year)
{
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

int daysInMonth(int month, int year)
{
    const int daysPerMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2 && isLeapYear(year))
    {
        return 29;
    }
    return daysPerMonth[month - 1];
}

long long convertToSeconds(int year, int month, int day, int hour, int minute, int second)
{
    const int secondsPerMinute = 60;
    const int secondsPerHour = 3600;
    const int secondsPerDay = 86400;

    long long totalSeconds = 0;

    // Calculate days from 1970 to the given year
    for (int y = 1970; y < year; y++)
    {
        totalSeconds += (isLeapYear(y) ? 366 : 365) * secondsPerDay;
    }

    // Calculate days for the months in the given year
    for (int m = 1; m < month; m++)
    {
        totalSeconds += daysInMonth(m, year) * secondsPerDay;
    }

    // Add days in the current month
    totalSeconds += (day - 1) * secondsPerDay;

    // Add hours, minutes, and seconds
    totalSeconds += hour * secondsPerHour;
    totalSeconds += minute * secondsPerMinute;
    totalSeconds += second;

    return totalSeconds;
}

int main()
{
    int n;
    scanf("%d", &n);

    file_count = n; // for count sort

    files = (File *)malloc(n * sizeof(File));

    int year, month, day, hour, minute, second;
    int total_seconds;
    max_id = -1;
    min_id = __INT_MAX__;
    max_time = -1;
    min_time = __INT_MAX__;
    max_nhash = -1;
    min_nhash = __INT_MAX__;
    for (int i = 0; i < n; i++)
    {
        scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp);

        if (files[i].id > max_id)
            max_id = files[i].id;
        if (files[i].id < min_id)
            min_id = files[i].id;

        // if (strptime(files[i].timestamp, "%Y-%m-%dT%H:%M:%S", &tm) == NULL)
        // {
        //     fprintf(stderr, "Failed to parse timestamp\n");
        //     return 1;
        // }
        if (sscanf(files[i].timestamp, "%4d-%2d-%2dT%2d:%2d:%2d", &year, &month, &day, &hour, &minute, &second) == 6)
        {
            total_seconds = convertToSeconds(year, month, day, hour, minute, second);
            // printf("Time in seconds since epoch: %lld\n", seconds);
        }
        else
        {
            printf("Error parsing timestamp\n");
            return 1;
        }

        files[i].timehash = total_seconds;
        if ((total_seconds) > max_time)
            max_time = total_seconds;
        if ((total_seconds) < min_time)
            min_time = total_seconds;

        files[i].nhash = namehash(files[i].name);
        // printf("%d\n",files[i].nhash);
        if (files[i].nhash > max_nhash)
            max_nhash = files[i].nhash;
        if (files[i].nhash < min_nhash)
            min_nhash = files[i].nhash;
    }

    char sortBy[20];
    scanf("%s", sortBy);

    printf("%s\n", sortBy);

    if (n < THRESHOLD)
    {
        sorted_files = malloc(file_count * sizeof(File));
        distributed_count_sort(sortBy);

        for (int i = 0; i < file_count; i++)
        {
            printf("%s %d %s\n", sorted_files[i].name, sorted_files[i].id, sorted_files[i].timestamp);
        }

        free(sorted_files);
        free(files);
        return 0;
    }
    else
    {
        parallelMergeSort(files, n, sortBy);
    }
    for (int i = 0; i < n; i++)
    {
        printf("%s %d %s\n", files[i].name, files[i].id, files[i].timestamp);
    }

    free(files);
    return 0;
}