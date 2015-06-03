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

#define WORDSIZE 256

enum CLICOMMAND
{
    MAP, REDUCE, EXIT, MARK, OTHER
};

void splitFile(char* input, size_t nClusters);
int parseCommand(const char* command);
size_t runMappers(char **tableNames, size_t nTables, size_t nProcesses,
                  pid_t* pidPool, size_t pidUsed, const char* processor,
                  int operationId);

int main(int argc, char** argv)
{
    char* tables[100];
    for (int i = 0; i < 100; i++)
        tables[i] = malloc(WORDSIZE);
    char processor[WORDSIZE];

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

//            if (0 == strcmp(tables[nTables - 1], "&"))  тут можно сделать additional

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

            if (1)  /// можно сделать additional здесь
            {
                int status = 0;
                for (size_t i = fromPid; i < fromPid + nTables * nProcesses; i++)
                    waitpid(childrenPids[i], &status, 0);
                if (fromPid + nTables * nProcesses == pidUsed)
                    pidUsed = fromPid;
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
        else
        {
            printf("Only \"map\" and \"reduce\" commands available\n");
            continue;
        }
    }

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
    for (size_t k = 0, i = 0; k < nClusters - 1; k++)
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
