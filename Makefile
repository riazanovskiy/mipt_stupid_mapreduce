CPPFLAGS= -std=gnu++1z -Weverything -Wno-missing-prototypes -Wno-global-constructors -Wno-c++98-compat


all: mapreduce processor_map processor_reduce

mapreduce: mapreduce.cpp
	clang++ mapreduce.cpp -o mapreduce $(CPPFLAGS) -lboost_system -lboost_filesystem

processor_reduce: processor_reduce.cpp
	clang++ processor_reduce.cpp -o processor_reduce $(CPPFLAGS)

processor_map: processor_map.cpp
	clang++ processor_map.cpp -o processor_map $(CPPFLAGS)
