#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
        if (nextToken != NULL)
        {
            strcpy(command, nextToken);
        }
        else
        {
            printf("No command found!\n");
            continue;
        }

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

            // here goes real shit

            if (strcmp(command, "map") == 0)
            {
                FILE** processorHandles = (FILE**) malloc(sizeof(FILE*) * nProcesses);
                char cmd[1000] = "";
                for (int i = 0; i < nProcesses; i++)
                {
                    snprintf(cmd, sizeof(cmd) - 1, "%s > temp_output_%i", processor, i);
                    processorHandles[i] = popen(cmd, "w");
                }

                int currentTableIdx = 0, currentProcessor = -1;
                FILE* currentInputTable = fopen(tables[currentTableIdx], "r");

                char *line = 0;
                size_t length = 0;

                while (currentTableIdx < nTables)
                {
                    if (feof(currentInputTable))
                    {
                        fclose(currentInputTable);
                        if (currentTableIdx + 1 < nTables)
                            currentInputTable = fopen(tables[++currentTableIdx], "r");
                        else
                            break;
                    }
                    getline(&line, &length, currentInputTable);
                    fwrite(line, strlen(line), 1, processorHandles[currentProcessor = ((currentProcessor + 1) % nProcesses)]);
                }

                for (int i = 0; i < nProcesses; i++)
                    pclose(processorHandles[i]);

                snprintf(cmd, sizeof(cmd) - 1, "cat temp_output_{0,%i} > %s", nProcesses - 1, output);
                system(cmd);
            }
            else if (strcmp(command, "reduce") == 0)
            {
                printf("here..\n");
                char bash_command[256];
                char tempFileName[] = "temp";
                snprintf(bash_command, sizeof(bash_command) - 1, "sort \"%s\" -o \"%s\"",
                         tempFileName, tempFileName);
                system(bash_command);
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
