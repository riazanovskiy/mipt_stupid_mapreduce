#include "reduce.h"

/************************** SORT *******************************/

char* getTempFilename(int operationID, int process)
{
    char* filename;
    asprintf(filename, "temp_%d_%d", operationID, process);
    return filename;
}

void readFirstString(char* filename, char** output)
{
    FILE* file = fopen(filename, "r");
    getline(output, NULL, file);
    fclose(file);
}

struct PartitionArgs
{
    char* filename;
    int operationID;
    char* delims[];
    size_t nChunks;
    pthread_mutex_t* chunkMutexes;
};

void* partitionFileWithDelims(void* args)
{
    char* filename = args->filename;
    int operationID = args->operationID;
    char* delims[] = args->delims;
    size_t nChunks = args->nChunks;

    FILE* file = fopen(filename, "r");

    char* line;
    size_t lineLength;

    while (getline(&line, &lineLength, file) >= 0)
    {
        size_t chunkID = 0;
        while (chunkID + 1 < nChunks && strcmp(line, delims[chunkID]) >= 0)
        {
            ++chunkID;
        }

        // START LOCK
        pthread_mutex_lock(&chunkMutexes[chunkID]);

        char* chunkFilename = getTempFilename(operationID, chunkID);
        FILE* chunkFile = fopen(chunkFilename, "a+");
        fprintf(chunkFile, "%s", line);
        close(chunkFile);
        free(chunkFilename);

        pthread_mutex_unlock(&chunkMutexes[chunkID]);
        // END LOCK
    }

}

/*char* filename;*/
/*int operationID;*/
/*char* delims[];*/
/*size_t nChunks;*/
/*pthread_mutex_t* chunkMutexes;*/
void emitToChunks(char* tableNames[], 
                  size_t nTables,
                  size_t nChunks, 
                  int operationID, 
                  char* delims[])
{
    // prepare mutexes and threads for each chunk
    pthread_t* chunkThreads;
    pthread_mutex_t* chunkMutexes;
    PartitionArgs* args;

    threads = malloc(sizeof(pthread_t) * nChunks);
    mutexes = malloc(sizeof(pthread_mutex_t) * nChunks);
    args    = malloc(sizeof(PartitionArgs) * nChunks);

    for (int idx = 0; idx < nChunks; ++idx)
        pthread_mutex_init(&mutexes[idx], 0);

    for (int idx = 0; idx < nChunks; ++idx)
    {
        args[idx]->filename = tableNames[idx]; // note: I'm not copying it is OK
        args[idx]->operationID = operationID;
        args[idx]->delims = delims;
        args[idx]->nChunks = nChunks;

        pthread_create(&chunkThreads[idx], NULL, partitionFileWithDelims, &args[idx]);
    }

    for (int idx = 0; idx < nChunks; ++idx)
        pthread_join(chunkThreads[idx], NULL);

    free(args);
    free(mutexes);
    free(threads);
}

void sortChunks(size_t nChunks, int operationID)
{

}

void getDelimiters(char* tableNames[], size_t nDelims, char* (*delims)[])
{
    // read delimiters as first strings from first tables
    *delims = malloc(sizeof(char*) * nDelims);
    for (int idx = 0; idx < nDelims; ++idx) 
    {
        readFirstString(tableNames[idx], (*delims)[idx]);
    }

    // sort delimiters
    qsort(delims, nTables, sizeof(char*), strcmp);
}

char* sortTables(char* tableNames[], size_t nTables)
{
    // generate file identifier not to collide with another temp file
    int operationID = rand();
    char* delims[];

    size_t nChunks = min(nProcesses, nTables);

    // get delimiters to partition tables to chunks
    getDelimiters(tableNames, nChunks - 1, operationID, &delims); 

    // partition tables to (nDelims + 1) chunks
    emitToChunks(tableNames, nTables, nChunks, operationID, delims);
    free(delims);

    // sort chunks
    sortChunks(nChunks, operationID);
}

/****************************** REDUCE ******************************/

void runReducers(char* tableNames[], size_t nTables, char* )
{
    sortTables(tables, nTables);
}
