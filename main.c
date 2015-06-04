#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "reduce.h"

enum CLICOMMAND
{
    MAP, REDUCE, EXIT, MARK, WRITE, WAIT, OTHER
};

void splitFile(char* input, size_t nClusters);
int parseCommand(const char* command);
size_t runMappers(char **tableNames, size_t nTables, size_t nProcesses, pid_t* pidPool,
                  size_t pidUsed, const char* processor, int operationId);
size_t collectChildren(size_t nProcesses, size_t pidUsed, pid_t *childrenPids, size_t nTables,
                       int operationId, char *output, size_t fromPid);

typedef struct
{
    size_t nTables, fromPid;
    char* outputTable;
    int operationId;
} DelayedOperation;

DelayedOperation* delayedOperations;

int main(int argc, char** argv)
{
    const int WORDSIZE = 256;
    char* tables[100];
    for (int i = 0; i < 100; i++)
        tables[i] = malloc(WORDSIZE);
    char* processor = calloc(WORDSIZE, 1);

    int delayedQueueSize = 100;
    delayedOperations = calloc(sizeof(DelayedOperation) * delayedQueueSize, 1);
    size_t nDelayedOperation = 0;

    system("ls");
    srand((unsigned int) time(0));
    size_t nProcesses = 4;

    if (argc > 1)
    {
        int nProcessesArgument = atoi(argv[1]);
        assert(nProcessesArgument > 0);
        nProcesses = (size_t) nProcessesArgument;
    }

    size_t nClusters = nProcesses;

    fprintf(stderr, "\nnProcesses: %zu \n", nProcesses);

    char *line = 0;
    size_t lineLength = 0;

    char command[WORDSIZE] = "";

    pid_t childrenPids[1000] = {};
    size_t pidUsed = 0;

    while (printf("1-day-till-deadline> "), getline(&line, &lineLength, stdin) >= 0)
    {
        char *nextToken = strtok(line, " \t\r\n");
        if (nextToken)
            strcpy(command, nextToken);
        else
            continue;

        int currentCommand = parseCommand(command);

        if (MAP == currentCommand || REDUCE == currentCommand)
        {
            if ((nextToken = strtok(NULL, " \t\r\n")))
            {
                strcpy(processor, nextToken);
            }
            else
            {
                fprintf(stderr, "No processor found!\n");
                continue;
            }

            size_t nTables = 0;
            while ((nextToken = strtok(NULL, " \t\r\n")))
                strcpy(tables[nTables++], nextToken);

            if (!nTables)
            {
                fprintf(stderr, "No output file specified\n");
                continue;
            }

            int asynchronous = 0;
            if (0 == strcmp(tables[nTables - 1], "&"))
            {
                asynchronous = 1;
                nTables--;
            }

            --nTables;
            char output[WORDSIZE];
            strcpy(output, tables[nTables]);

            int operationId = rand();

            size_t fromPid = pidUsed;

            if (REDUCE == currentCommand)
                pidUsed = runReducers(tables, nTables, nProcesses, childrenPids, pidUsed,
                                      processor, operationId);
            else
                pidUsed = runMappers(tables, nTables, nProcesses, childrenPids, pidUsed,
                                     processor, operationId);
            if (!asynchronous)
            {
                pidUsed = collectChildren(nProcesses, pidUsed, childrenPids, nTables,
                                          operationId, output, fromPid);
            }
            else
            {
                if (nDelayedOperation + 1 < delayedQueueSize)
                {
                    printf("Operation %zu in background\n", nDelayedOperation);
                    delayedOperations[nDelayedOperation].fromPid = fromPid;
                    delayedOperations[nDelayedOperation].nTables = nTables;
                    delayedOperations[nDelayedOperation].operationId = operationId;
                    delayedOperations[nDelayedOperation].outputTable = strdup(output);
                    nDelayedOperation++;
                }
                else
                {
                    fprintf(stderr, "Can not run in background\n");
                    pidUsed = collectChildren(nProcesses, pidUsed, childrenPids, nTables,
                                              operationId, output, fromPid);
                }
            }
        }
        else if (EXIT == currentCommand)
        {
            break;
        }
        else if (MARK == currentCommand)
        {
            char input[1000] = "";
            if ((nextToken = strtok(NULL, " \t\r\n")))
            {
                strcpy(input, nextToken);
            }
            else
            {
                fprintf(stderr, "No filename given\n");
                continue;
            }

            splitFile(input, nClusters);
        }
        else if (WAIT == currentCommand)
        {
            while (nDelayedOperation)
            {
                nDelayedOperation--;
                pidUsed = collectChildren(nProcesses, pidUsed, childrenPids,
                                          delayedOperations[nDelayedOperation].nTables,
                                          delayedOperations[nDelayedOperation].operationId,
                                          delayedOperations[nDelayedOperation].outputTable,
                                          delayedOperations[nDelayedOperation].fromPid);
                free(delayedOperations[nDelayedOperation].outputTable);
            }
        }
        else
        {
            printf("Only \"map\" and \"reduce\" commands available\n");
            continue;
        }
    }

    free(delayedOperations);
    free(processor);
    for (int i = 0; i < 100; i++)
        free(tables[i]);
    free(line);

    return 0;
}

void splitFile(char* input, size_t nClusters)
{
    strcat(input, ".tbl");

    FILE* inputFile = fopen(input, "r");
    if (!input)
    {
        fprintf(stderr, "Can not open file %s\n", input);
        return;
    }
    fseek(inputFile, 0, SEEK_END);
    size_t fileLength = (size_t) ftell(inputFile);

    char* fileAddr = (char*) mmap(0, fileLength, PROT_READ, MAP_SHARED,
                                  fileno(inputFile), 0);

    size_t labels[nClusters + 1];
    memset(labels, 0, (nClusters + 1) * sizeof(labels[0]));

    size_t currCluster = 0;
    for (size_t k = 0, i = (fileLength / nClusters) * 0.9; k < nClusters - 1; k++)
    {
        for (size_t j = 0; i < fileLength && j < (fileLength / nClusters + 1); i++, j++)
        {
            if (fileAddr[i] == '\n')
            {
                labels[currCluster++] = i;
                i += 2 + fileLength / nClusters - j; // есть ли тут ошибка на один? Да наверняка
                break;
            }
        }
    }

    labels[currCluster++] = fileLength;
//    nClusters = currCluster; тут (мб) надо делать меньше блоков, если нужно. сейчас делаются нулевые.

    munmap(fileAddr, fileLength);
    fclose(inputFile);

    for (ssize_t i = 0, prevOffset = -1;
         i < (ssize_t) nClusters;
         prevOffset = (int64_t) labels[i], i++)
    {
        char cmd[1000] = "";
        snprintf(cmd, sizeof(cmd) - 1, "dd bs=1 if=%s skip=%zi count=%zu of=marked_%s_%zi",
                 input, prevOffset + 1, (ssize_t) labels[i] - prevOffset, input, i);
        system(cmd);
    }
}

int parseCommand(const char* command)
{
    int currentCommand = OTHER;
    if (strcmp(command, "map") == 0)
        currentCommand = MAP;
    else if (strcmp(command, "reduce") == 0)
        currentCommand = REDUCE;
    else if (strcmp(command, "exit") == 0)
        currentCommand = EXIT;
    else if (strcmp(command, "mark") == 0)
        currentCommand = MARK;
    else if (strcmp(command, "wait") == 0)
        currentCommand = WAIT;
    else if (strcmp(command, "write") == 0)
        currentCommand = WRITE;

    return currentCommand;
}

size_t runMappers(char **tableNames, size_t nTables, size_t nProcesses,
                  pid_t* pidPool, size_t pidUsed, const char* processor,
                  int operationId)
{
    char cmd[1000];
    for (size_t currTable = 0, outputTableIdx = 0; currTable < nTables; currTable++)
    {
        for (size_t i = 0; i < nProcesses; i++, outputTableIdx++, pidUsed++)
        {
            pidPool[pidUsed] = fork();
            if (0 == pidPool[pidUsed])
            {
                snprintf(cmd, sizeof(cmd) - 1,
                         "%s < marked_%s.tbl_%zu > temp_%i_%zu",
                         processor, tableNames[currTable], i, operationId, outputTableIdx);
                execlp("bash", "bash", "-c", cmd, (char*)(NULL));
                return 0;
            }
        }
    }
    return pidUsed;
}

size_t collectChildren(size_t nProcesses, size_t pidUsed, pid_t* childrenPids, size_t nTables, int operationId,
                       char* output, size_t fromPid)
{

    int status = 0;
    for (size_t i = fromPid; i < fromPid + nTables * nProcesses; i++)
    {
        waitpid(childrenPids[i], &status, 0);
        childrenPids[i] = 0;
    }

    if (fromPid + nTables * nProcesses == pidUsed)
    {
        pidUsed = fromPid;
        while (pidUsed > 0 && !childrenPids[pidUsed])
            pidUsed--;
    }


    for (size_t i = 0; i < nProcesses; i++)
    {
        char cmd[1000] = "cat ";
        int from = (int) strlen(cmd);
        for (size_t j = 0; j < nTables; j++)
        {
            from += snprintf(cmd + from, sizeof(cmd) - 1, "temp_%i_%zu ", operationId, i + nProcesses*j);
            assert((size_t)from < sizeof(cmd));
        }
        snprintf(cmd + from, sizeof(cmd) - 1, "> marked_%s.tbl_%zu", output, i);
        system(cmd);
    }

    char cmd[1000] = "";
    snprintf(cmd, sizeof(cmd) - 1, "bash -c \"rm temp_%i_{0..%zu}\"", operationId, nProcesses * nTables - 1);
    system(cmd);

    return pidUsed;
}
