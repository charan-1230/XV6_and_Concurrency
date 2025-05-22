#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

#define YELLOW "\033[33m"
#define PINK "\033[35m"
#define WHITE "\033[37m"
#define GREEN "\033[32m"
#define RED "\033[31m"
#define RESET "\033[0m"

typedef struct
{
    int u;
    int f;
    char o[10];
    int t;
} Request;

typedef struct
{
    int access_count; // Number of concurrent accesses
    int write_flag;   // 1 if someone is writing to file,else 0
    int delete_flag;  // 1 if someone deleted the file,else 0
    pthread_mutex_t lock;
    pthread_cond_t read;
    pthread_cond_t write;
    pthread_cond_t delete;
} FileStatus;

Request requests[1024];
FileStatus *files;
int req_cnt = 0;
int r, w, d, n, c, T;
int base_time;

void *execute_req(void *arg)
{
    Request *req = (Request *)arg;

    sleep(req->t);
    // curr_time += req->t;

    printf(YELLOW "User %d has made request for performing %s on file %d at %ld seconds\n" RESET,
           req->u, req->o, req->f, time(NULL) - base_time);

    // lazy waits for 1 sec to process request
    sleep(1);
    // curr_time += 1;

    FileStatus *file = &files[req->f - 1];
    pthread_mutex_lock(&file->lock);
    int curr_time = req->t + 1;
    if (file->delete_flag)
    {
        printf(WHITE "LAZY has declined the request of User %d at %ld seconds because an deleted file was requested.\n" RESET, req->u, time(NULL) - base_time);
    }
    else if (strcmp(req->o, "READ") == 0)
    {
        time_t start_time;
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += T - 1;
        // ts.tv_sec = time(NULL) + T - 1;
        while (file->access_count >= c)
        {
            // start_time = time(NULL);
            int rc = pthread_cond_timedwait(&file->read, &file->lock, &ts);
            if (rc == ETIMEDOUT)
            {
                printf(RED "User %d canceled the request due to no response at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
                pthread_mutex_unlock(&file->lock);
                return NULL;
            }
            // curr_time += (int)(time(NULL) - start_time);
        }
        file->access_count++;

        printf(PINK "LAZY has taken up the request of User %d at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
        pthread_mutex_unlock(&file->lock);

        sleep(r); // read happens

        pthread_mutex_lock(&file->lock);
        (file->access_count)--;

        printf(GREEN "The request for User %d was completed at %ld seconds\n" RESET, req->u, time(NULL) - base_time);

        pthread_cond_signal(&file->read);
        if (file->write_flag == 0)
        {
            pthread_cond_signal(&file->write);
            if (file->access_count == 0)
                pthread_cond_signal(&file->delete);
        }
    }
    else if (strcmp(req->o, "WRITE") == 0)
    {
        time_t start_time;
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += T - 1;
        // ts.tv_sec = time(NULL) + T - 1;
        while (file->access_count >= c || file->write_flag)
        {
            // start_time = time(NULL);
            int rc = pthread_cond_timedwait(&file->write, &file->lock, &ts);
            if (rc == ETIMEDOUT)
            {
                printf(RED "User %d canceled the request due to no response at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
                pthread_mutex_unlock(&file->lock);
                return NULL;
            }
            // curr_time += (int)(time(NULL) - start_time);
        }

        file->write_flag = 1;
        file->access_count++;

        printf(PINK "LAZY has taken up the request of User %d at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
        pthread_mutex_unlock(&file->lock);

        sleep(w); // write happens

        pthread_mutex_lock(&file->lock);
        file->write_flag = 0;
        file->access_count--;

        printf(GREEN "The request for User %d was completed at %ld seconds\n" RESET, req->u, time(NULL) - base_time);

        pthread_cond_signal(&file->write);
        if (file->access_count == 0)
            pthread_cond_signal(&file->delete);
    }
    else if (strcmp(req->o, "DELETE") == 0)
    {
        time_t start_time;
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += T - 1;
        // ts.tv_sec = time(NULL) + T - 1;
        while (file->access_count > 0)
        {
            // start_time = time(NULL) - base_time;
            // printf("%ld\n", start_time);
            int rc = pthread_cond_timedwait(&file->delete, &file->lock, &ts);
            // printf("%ld\n",time(NULL)-base_time);
            if (rc == ETIMEDOUT)
            {
                printf(RED "User %d canceled the request due to no response at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
                pthread_mutex_unlock(&file->lock);
                return NULL;
            }
            // curr_time += (int)(time(NULL) - start_time);
        }

        file->delete_flag = 1;
        printf(PINK "LAZY has taken up the request of User %d at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
        pthread_mutex_unlock(&file->lock);

        sleep(d); // delete happens

        pthread_mutex_lock(&file->lock);
        printf(GREEN "The request for User %d was completed at %ld seconds\n" RESET, req->u, time(NULL) - base_time);
    }
    pthread_mutex_unlock(&file->lock);
    return NULL;
}

int main()
{
    scanf("%d %d %d", &r, &w, &d);
    scanf("%d %d %d", &n, &c, &T);

    files = (FileStatus *)malloc(n * sizeof(FileStatus));
    for (int i = 0; i < n; i++)
    {
        files[i].access_count = 0;
        files[i].write_flag = 0;
        files[i].delete_flag = 0;
        pthread_mutex_init(&files[i].lock, NULL);
        pthread_cond_init(&files[i].read, NULL);
        pthread_cond_init(&files[i].write, NULL);
        pthread_cond_init(&files[i].delete, NULL);
    }

    while (1)
    {
        char line[100];
        fgets(line, sizeof(line), stdin);
        if (strlen(line) == 1)
        {
            continue;
        }
        else if (strncmp(line, "STOP", 4) == 0)
        {
            break;
        }
        if (sscanf(line, "%d %d %9s %d", &requests[req_cnt].u, &requests[req_cnt].f, requests[req_cnt].o, &requests[req_cnt].t) == 4)
        {
            if (requests[req_cnt].f > n)
            {
                printf(RED "Error: There are only %d files" RESET, n);
            }
            else
            {
                if (strcmp(requests[req_cnt].o, "READ") == 0 ||
                    strcmp(requests[req_cnt].o, "WRITE") == 0 ||
                    strcmp(requests[req_cnt].o, "DELETE") == 0)
                {
                    req_cnt++;
                    if (req_cnt >= 1024)
                    {
                        printf(RED "Maximum number of requests reached.\n" RESET);
                        break;
                    }
                }
                else
                {
                    printf(RED "Invalid operation. Please enter 'READ', 'WRITE', or 'DELETE' for the operation.\n" RESET);
                }
            }
        }
        else
        {
            printf(RED "Invalid input format. Make sure 'u' and 'f' and 't' are integers.\n" RESET);
        }
    }

    // printf("\nYou entered:\n");
    // for (int i = 0; i < req_cnt; i++)
    // {
    //     printf("%d, %d, %s, %d\n",
    //            requests[i].u, requests[i].f, requests[i].o, requests[i].t);
    // }
    printf("\n");
    printf("LAZY has woken up!\n");

    base_time = time(NULL);

    pthread_t threads[1024];
    for (int i = 0; i < req_cnt; i++)
    {
        pthread_create(&threads[i], NULL, execute_req, &requests[i]);
    }

    for (int i = 0; i < req_cnt; i++)
    {
        pthread_join(threads[i], NULL);
    }

    printf("LAZY has no more pending requests and is going back to sleep!\n");

    for (int i = 0; i < n; i++)
    {
        pthread_mutex_destroy(&files[i].lock);
        pthread_cond_destroy(&files[i].read);
        pthread_cond_destroy(&files[i].write);
        pthread_cond_destroy(&files[i].delete);
    }

    free(files);
    return 0;
}