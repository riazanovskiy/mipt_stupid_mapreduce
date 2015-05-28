#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define WORDSIZE 256

char processor[WORDSIZE];

char tables[100][WORDSIZE];

enum CLICOMMAND
{
    MAP, REDUCE, EXIT, MARK, OTHER
};


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
    nClusters = currCluster;

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

int main(int argc, char** argv)
{
    srand((uint)time(0));
    system("ls");
    size_t nProcesses = 4;

    if (argc > 1)
    {
        int nProcessesArgument = atoi(argv[1]);
        assert(nProcessesArgument > 0);
        nProcesses = (size_t) nProcessesArgument;
    }

    size_t nClusters = nProcesses;

    FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);
    char *line = 0;
    size_t lineLength = 0;

    char command[WORDSIZE] = "";
    char cmd[1000] = "";

    pid_t childrenPids[1000] = {};

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

            --nTables;
            char output[WORDSIZE];
            strcpy(output, tables[nTables]);

            if (REDUCE == currentCommand)
            {
                assert(REDUCE != currentCommand);
                for (int i = 0; i < nTables; ++i)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "sort --parallel=%lu %s > reduce_%i.tbl",
                             nProcesses, tables[i], i);
                    system(cmd);
                }

                char tempName[256];
                snprintf(cmd, sizeof(cmd) - 1, "sort --merge");
                for (int i = 0; i < nTables; ++i)
                {
                    snprintf(tempName, sizeof(tempName) - 1, " reduce_%i.tbl", i);
                    strncat(cmd, tempName, sizeof(cmd) - 1);
                }
                strncat(cmd, " > sorted.tbl", sizeof(cmd) - 1);
                system(cmd);

                strcpy(tables[0], "sorted.tbl");
                nTables = 1;
            }

            for (size_t i = 0; i < nProcesses; i++)
            {
                snprintf(cmd, sizeof(cmd) - 1, "%s > temp_%s_%lu.tbl", processor, command, i);
                processorHandles[i] = popen(cmd, "w");
            }

            int currentTableIdx = 0, currentProcessor = 0;
            FILE* currentInputTable = fopen(tables[currentTableIdx], "r");

            char lastKey[WORDSIZE] = "Please work";
            char key[WORDSIZE] = "";

            do
            {
                if (!currentInputTable)
                {
                    fprintf(stderr, "File %s could not be opened, exiting\n", tables[currentTableIdx]);
                    break;
                }
                while (!feof(currentInputTable))
                {
                    getline(&line, &lineLength, currentInputTable);
                    fwrite(line, strlen(line), 1, processorHandles[currentProcessor]);

                    if (currentCommand == MAP || (sscanf(line, "%s", key)
                                                  && strcmp(lastKey, key)
                                                  && strcpy(lastKey, key)))
                        currentProcessor = (currentProcessor + 1) % (int)nProcesses;
                }
                fclose(currentInputTable);
            }
            while (currentTableIdx < nTables - 1 && (currentInputTable = fopen(tables[++currentTableIdx], "r")));

            snprintf(cmd, sizeof(cmd) - 1, "cat temp_%s_{0..%lu}.tbl > %s", command, nProcesses - 1, output);
            system(cmd);

            for (size_t i = 0; i < nProcesses; i++)
                pclose(processorHandles[i]);
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

    snprintf(cmd, sizeof(cmd) - 1, "rm -f temp_{map,reduce}_{0..%lu}.tbl", nProcesses - 1);
    system(cmd);

    free(processorHandles);
    free(line);

    return 0;
}
