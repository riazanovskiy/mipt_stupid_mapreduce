#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define LINESIZE 256
#define WORDSIZE 256

char line[LINESIZE];

char command[WORDSIZE];
char processor[WORDSIZE];

char tables[100][WORDSIZE];
char output[WORDSIZE];

size_t nProcesses = 4;

int main()
{
    while (printf("1-day-till-deadline> "), fgets(line, LINESIZE, stdin))
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
                FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);

                for (size_t i = 0; i < nProcesses; i++)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "%s > temp/temp_%s_%lu.tbl", processor, command, i);
                    processorHandles[i] = popen(cmd, "w");
                }

                int currentTableIdx = 0, currentProcessor = -1;
                FILE* currentInputTable = fopen(tables[currentTableIdx], "r");

                char *line = 0;
                size_t length = 0;

                do
                {
                    assert(currentInputTable);
                    while (!feof(currentInputTable))
                    {
                        getline(&line, &length, currentInputTable);
                        fwrite(line, strlen(line), 1,
                               processorHandles[currentProcessor = ((currentProcessor + 1) % (int) nProcesses)]);
                    }
                    fclose(currentInputTable);
                }
                while (currentTableIdx < nTables - 1 &&
                        (currentInputTable = fopen(tables[++currentTableIdx], "r")));

                for (size_t i = 0; i < nProcesses; i++)
                    pclose(processorHandles[i]);

                snprintf(cmd, sizeof(cmd) - 1, "cat temp/temp_%s_{0..%lu}.tbl > %s", command, nProcesses - 1, output);
                system(cmd);
            }
            else if (strcmp(command, "reduce") == 0)
            {
                for (int i = 0; i < nTables; ++i)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "sort --parallel=%lu %s > temp/reduce_%i.tbl",
                             nProcesses, tables[i], i);
                    system(cmd);
                }

                char tempName[LINESIZE];
                snprintf(cmd, sizeof(cmd) - 1, "sort --merge");
                for (int i = 0; i < nTables; ++i)
                {
                    snprintf(tempName, sizeof(tempName) - 1, " temp/reduce_%i.tbl", i);
                    strncat(cmd, tempName, sizeof(cmd) - 1);
                }
                strncat(cmd, " > temp/sorted.tbl", sizeof(cmd) - 1);

                fprintf(stderr, "whole command: %s\n", cmd);
                system(cmd);

                // now all is sorted and stored in output

                FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);

                // run reducers
                for (size_t i = 0; i < nProcesses; i++)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "%s > reduce_answer_%lu.tbl", processor, i);
                    processorHandles[i] = popen(cmd, "w");
                }

                int currentProcessor = -1;
                FILE* reducersInput = fopen("temp/sorted.tbl", "r");
                assert(reducersInput);

                fprintf(stderr, "temp/sorted.tbl was created and opened\n");

                // all string are sorted and are in output

                char *line = 0;
                size_t length = 0;

                char lastKey[WORDSIZE] = "Please work";
                char key[WORDSIZE] = "";
                while (!feof(reducersInput))
                {
                    getline(&line, &length, reducersInput);
                    sscanf(line, "%s", key);

                    if (strcmp(lastKey, key) != 0)
                        currentProcessor = ((currentProcessor + 1) % (int)nProcesses);
                    strcpy(lastKey, key);

                    fwrite(line, strlen(line), 1, processorHandles[currentProcessor]);
                }

                for (size_t i = 0; i < nProcesses; i++)
                    pclose(processorHandles[i]);

                fclose(reducersInput);
            }
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

    return 0;
}
