#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define WORDSIZE 256

char processor[WORDSIZE];

char tables[100][WORDSIZE];
char output[WORDSIZE];

enum CLICOMMAND
{
    MAP, REDUCE, EXIT, OTHER
};

int main(int argc, char** argv)
{
    size_t nProcesses = 4;

    if (argc > 1)
    {
        int nProcessesArgument = atoi(argv[1]);
        assert(nProcessesArgument > 0);
        nProcesses = (size_t) nProcessesArgument;
    }

    FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);
    char *line = 0;
    size_t lineLength = 0;

    char command[WORDSIZE] = "";
    char cmd[1000] = "";

    while (printf("1-day-till-deadline> "), getline(&line, &lineLength, stdin) >= 0)
    {
        char *nextToken = strtok(line, " \t\r\n");
        if (nextToken)
            strcpy(command, nextToken);
        else
            continue;

        int currentCommand = OTHER;
        if (strcmp(command, "map") == 0)
            currentCommand = MAP;
        else if (strcmp(command, "reduce"))
            currentCommand = REDUCE;
        else if (strcmp(command, "exit"))
            currentCommand = EXIT;

        if (MAP == currentCommand || REDUCE == currentCommand)
        {
            if (nextToken = strtok(NULL, " \t\r\n"))
            {
                strcpy(processor, nextToken);
            }
            else
            {
                printf("No processor found!\n");
                continue;
            }

            int nTables = 0;
            while (nextToken = strtok(NULL, " \t\r\n"))
            {
                strcpy(tables[nTables], nextToken);
                strncat(tables[nTables], ".tbl", WORDSIZE);
                nTables++;
            }

            if (!nTables)
            {
                printf("No output file specified\n");
                continue;
            }

            --nTables;
            strcpy(output, tables[nTables]);

            if (REDUCE == currentCommand)
            {
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
