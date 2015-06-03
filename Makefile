CPPFLAGS= -std=gnu++1z -Weverything -Wno-missing-prototypes -Wno-global-constructors -Wno-c++98-compat

CFLAGS=-Weverything -std=gnu11 -Wno-missing-variable-declarations -Wno-missing-prototypes \
		-Wno-gnu-empty-initializer -Wno-vla -D_GNU_SOURCE -pthread

main:main.o reduce.o
	clang main.o reduce.o -o main  -pthread -lbsd

all: mapreduce processor_map processor_reduce main

mapreduce: mapreduce.cpp
	clang++ mapreduce.cpp -o mapreduce $(CPPFLAGS) -lboost_system -lboost_filesystem

processor_reduce: processor_reduce.cpp
	clang++ processor_reduce.cpp -o processor_reduce $(CPPFLAGS)

processor_map: processor_map.cpp
	clang++ processor_map.cpp -o processor_map $(CPPFLAGS)

main.o:
	clang -c main.c -o main.o $(CFLAGS)

reduce.o:
	clang -c reduce.c -o reduce.o $(CFLAGS)

clean:
	rm -f *.o mapreduce processor_map processor_reduce main
