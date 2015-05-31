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
    char cmd[1000] = "";

    pid_t childrenPids[1000] = {};
    int pidUsed = 0;

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

            int nTables = 0;
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

            if (REDUCE == currentCommand)
                sortTables(tables, nTables, nProcesses);

            int outputTableIdx = 0, operationId = rand();
            int fromPid = pidUsed;

            for (int currTable = 0; currTable < nTables; currTable++)
            {
                for (size_t i = 0; i < nProcesses; i++)
                {
                    childrenPids[pidUsed] = fork();
                    if (0 == childrenPids[pidUsed])
                    {
                        snprintf(cmd, sizeof(cmd) - 1,
                                 "%s < marked_%s.tbl_%zu > temp_%i_%i",
                                 processor, tables[currTable], i, operationId, outputTableIdx);
                        execlp("bash", "bash", "-c", cmd, (char*)(NULL));
                        return 0;
                    }
                    outputTableIdx++;
                    pidUsed++;
                }
            }

            if (1)  /// можно сделать additional здесь
            {
                int status = 0;
                for (int i = fromPid; i < fromPid + outputTableIdx; i++)
                    waitpid(childrenPids[i], &status, 0);
                if (fromPid + outputTableIdx == pidUsed)
                    pidUsed = fromPid;
            }

            for (int i = 0; i < nProcesses; i++)
            {
                char cmd[1000] = "cat ";
                int from = strlen(cmd);
                for (int j = 0; j < nTables; j++)
                {
                    from += snprintf(cmd + from, sizeof(cmd) - 1, "temp_%i_%i ", operationId, i + nProcesses*j);
                    assert(from < sizeof(cmd));
                }
                snprintf(cmd + from, sizeof(cmd) - 1, "> marked_%s.tbl_%i", output, i);
                system(cmd);
            }

            snprintf(cmd, sizeof(cmd) - 1, "bash -c \"rm temp_%i_{0..%i}\"", operationId, outputTableIdx - 1);
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
    strncat(input, ".tbl", sizeof(input) - 1);

    FILE* inputFile = fopen(input, "r");
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

    for (int i = 0, prevOffset = -1; i < nClusters; prevOffset = labels[i], i++)
    {
        char cmd[1000] = "";
        snprintf(cmd, sizeof(cmd) - 1, "dd bs=1 if=%s skip=%i count=%zu of=marked_%s_%i",
                 input, prevOffset + 1, labels[i] - prevOffset, input, i);
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
