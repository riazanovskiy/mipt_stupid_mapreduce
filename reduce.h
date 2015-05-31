#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <pthread.h>

size_t runReducers(char **tableNames, size_t nTables, size_t nProcesses,
                   pid_t* pidPool, size_t pidUsed, const char* processor,
                   int operationId);
