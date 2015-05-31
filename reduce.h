#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <pthread.h>

char* getTempFilename(int operationID, size_t process);
void emitToChunks(char* tableNames[], size_t nTables, size_t nProcesses, int operationID, char* delims[]);
void sortChunks(size_t nChunks, int operationID);
char** getDelimiters(char* tableNames[], size_t nTables, size_t nProcesses);
void sortTables(char **tableNames, size_t nTables, size_t nProcesses);
//void runReducers(char* tableNames[], size_t nTables, char* )
