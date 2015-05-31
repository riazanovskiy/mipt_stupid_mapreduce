#include "reduce.h"


/************************** SORT *******************************/

char* getTempFilename(int operationID, size_t process)
{
    char* filename;
    asprintf(&filename, "temp_%d_%zu", operationID, process);
    return filename;
}

void readFirstString(char* filename, char** output)
{
    FILE* file = fopen(filename, "r");
    size_t n = 0;
    getline(output, &n, file);
    fclose(file);
}

typedef struct
{
    char* inputFileName;
    char** delims;
    size_t nChunks;
    pthread_mutex_t* chunkMutexes;
    int operationID;
} PartitionArgs;

void* partitionFileWithDelims(PartitionArgs* args)
{
    FILE* file = fopen(args->inputFileName, "r");

    char* line = 0;
    size_t lineLength = 0;

    while (getline(&line, &lineLength, file) >= 0)
    {
        size_t chunkID = 0;
        while (chunkID + 1 < args->nChunks && strcmp(line, args->delims[chunkID]) >= 0)
            ++chunkID;

        // START LOCK
        pthread_mutex_lock(&args->chunkMutexes[chunkID]);

        char* chunkFilename = getTempFilename(args->operationID, chunkID);
        FILE* chunkFile = fopen(chunkFilename, "a+");
        fprintf(chunkFile, "%s", line);
        fclose(chunkFile);
        free(chunkFilename);

        pthread_mutex_unlock(&args->chunkMutexes[chunkID]);
        // END LOCK
    }

    free(line);

    return 0;
}

void emitToChunks(char** tableNames, size_t nTables, size_t nProcesses,  int operationID, char* delims[])
{
    size_t nChunks = nTables * nProcesses;
    // prepare mutexes and threads for each chunk
    pthread_t* chunkThreads = malloc(sizeof(pthread_t) * nChunks);
    pthread_mutex_t* chunkMutexes = malloc(sizeof(pthread_mutex_t) * nChunks);
    PartitionArgs* args = malloc(sizeof(PartitionArgs) * nChunks);

    for (size_t idx = 0; idx < nChunks; ++idx)
        pthread_mutex_init(&chunkMutexes[idx], 0);

    for (size_t table = 0, idx = 0; table < nTables; table++)
    {
        for (size_t part = 0; part < nProcesses; part++, idx++)
        {
            args[idx].chunkMutexes = chunkMutexes;
            args[idx].delims = delims;
            asprintf(&args[idx].inputFileName, "marked_%s.tbl_%zu", tableNames[table], part);
            args[idx].operationID = operationID;
            args[idx].nChunks = nChunks;

            pthread_create(chunkThreads + idx, NULL, partitionFileWithDelims, args + idx);
        }
    }

    for (size_t idx = 0; idx < nChunks; ++idx)
        pthread_join(chunkThreads[idx], NULL);

    for (size_t i = 0; i < nChunks; i++)
        free(args[i].inputFileName);

    free(args);
    free(chunkMutexes);
    free(chunkThreads);
}

void sortChunks(size_t nChunks, int operationID)
{
    pid_t pids[nChunks];
    for (size_t currentChunk = 0; currentChunk < nChunks; currentChunk++)
    {
        pids[currentChunk] = fork();
        if (0 == pids[currentChunk])
        {
            char cmd[1000];
            snprintf(cmd, sizeof(cmd) - 1, "temp_%i_%zu", operationID, currentChunk);
            execlp("sort", "sort", cmd, "-o", cmd, (char*)(NULL));
        }
    }
    int status = 0;
    for (size_t i = 0; i < nChunks; i++)
        waitpid(pids[i], &status, 0);
}

char** getDelimiters(char* tableNames[], size_t nTables, size_t nProcesses)
{
    // read delimiters as first strings from first tables
    size_t nDelims = nTables * nProcesses;
    char** delims = calloc(sizeof(char*) * nDelims, 1);

    char filename[1000];
    for (size_t table = 0; table < nTables; table++)
    {
        for (size_t i = 0; i < nProcesses; ++i)
        {
            snprintf(filename, sizeof(filename) - 1, "marked_%s.tbl_%zu",
                     tableNames[table], i);
            readFirstString(filename, delims + i + table*nTables);
        }
    }

    qsort(delims, nDelims, sizeof(char*), strcmp);

    return delims;
}

void sortTables(char **tableNames, size_t nTables, size_t nProcesses)
{
    // generate file identifier not to collide with another temp file
    int operationID = rand();

    // get delimiters to partition tables to chunks
    char** delimiters = getDelimiters(tableNames, nTables, nProcesses);

    // partition tables to (nDelims + 1) chunks
    emitToChunks(tableNames, nTables, nProcesses, operationID, delimiters);
    free(delimiters);

    // sort chunks
    sortChunks(nProcesses, operationID);
}

/****************************** REDUCE ******************************/

//void runReducers(char* tableNames[], size_t nTables, char* )
//{
//    sortTables(tables, nTables);
//}

