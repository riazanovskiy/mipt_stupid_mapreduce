#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define WORDSIZE 256

char command[WORDSIZE];
char processor[WORDSIZE];

char tables[100][WORDSIZE];
char output[WORDSIZE];

size_t nProcesses = 4;

int main()
{
    FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);
    char *line = 0;
    size_t lineLength = 0;

    while (printf("1-day-till-deadline> "), getline(&line, &lineLength, stdin) >= 0)
    {
        char *nextToken = strtok(line, " \t\r\n");
        if (nextToken)
            strcpy(command, nextToken);
        else
            continue;

        if (strcmp(command, "map") == 0 || strcmp(command, "reduce") == 0)
        {
            if ((nextToken = strtok(NULL, " \t\r\n")))
            {
                strcpy(processor, nextToken);
            }
            else
            {
                printf("No processor found!\n");
                continue;
            }

            int nTables = 0;
            while ((nextToken = strtok(NULL, " \t\r\n")))
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

            char cmd[1000] = "";

            if (strcmp(command, "map") == 0)
            {
                for (size_t i = 0; i < nProcesses; i++)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "%s > temp_%s_%lu.tbl", processor, command, i);
                    processorHandles[i] = popen(cmd, "w");
                }

                int currentTableIdx = 0, currentProcessor = -1;
                FILE* currentInputTable = fopen(tables[currentTableIdx], "r");

                do
                {
                    assert(currentInputTable);
                    while (!feof(currentInputTable))
                    {
                        getline(&line, &lineLength, currentInputTable);
                        fwrite(line, strlen(line), 1,
                               processorHandles[currentProcessor = ((currentProcessor + 1) % (int) nProcesses)]);
                    }
                    fclose(currentInputTable);
                }
                while (currentTableIdx < nTables - 1 &&
                        (currentInputTable = fopen(tables[++currentTableIdx], "r")));
            }
            else if (strcmp(command, "reduce") == 0)
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

                fprintf(stderr, "whole command: %s\n", cmd);
                system(cmd);

                // now all is sorted and stored in output

                // run reducers
                for (size_t i = 0; i < nProcesses; i++)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "%s > temp_%s_%lu.tbl", processor, command, i);
                    processorHandles[i] = popen(cmd, "w");
                }

                int currentProcessor = -1;
                FILE* reducersInput = fopen("sorted.tbl", "r");
                assert(reducersInput);

                fprintf(stderr, "sorted.tbl was created and opened\n");

                char lastKey[WORDSIZE] = "Please work";
                char key[WORDSIZE] = "";
                while (!feof(reducersInput))
                {
                    getline(&line, &lineLength, reducersInput);
                    sscanf(line, "%s", key);

                    if (strcmp(lastKey, key) != 0)
                        currentProcessor = ((currentProcessor + 1) % (int)nProcesses);
                    strcpy(lastKey, key);

                    fwrite(line, strlen(line), 1, processorHandles[currentProcessor]);
                }

                fclose(reducersInput);
            }

            snprintf(cmd, sizeof(cmd) - 1, "cat temp_%s_{0..%lu}.tbl > %s", command, nProcesses - 1, output);
            system(cmd);

            for (size_t i = 0; i < nProcesses; i++)
                pclose(processorHandles[i]);
        }
        else if (strcmp(command, "exit") == 0)
        {
            break;
        }
        else
        {
            printf("Only \"map\" and \"reduce\" commands available\n");
            continue;
        }
    }

    free(processorHandles);
    free(line);

    return 0;
}
