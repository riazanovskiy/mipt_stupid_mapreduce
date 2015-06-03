#ifndef REDUCE_H_
#define REDUCE_H_

size_t runReducers(char **tableNames, size_t nTables, size_t nProcesses,
                   pid_t* pidPool, size_t pidUsed, const char* processor,
                   int operationId);

#endif
