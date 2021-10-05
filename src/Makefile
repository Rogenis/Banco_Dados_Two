# script para compilar os programas

all: directories upload findrec seek

directories:
	mkdir -p objects
	mkdir -p datafiles
	mkdir -p indexes

findrec:  objects/records.o objects/util.o objects/findrec.o
	g++ -std=c++11  -o findrec objects/records.o objects/btree.o objects/util.o objects/findrec.o

upload: objects/records.o  objects/btree.o objects/util.o objects/upload.o
	g++ -std=c++11 -o upload  objects/records.o   objects/btree.o objects/util.o objects/upload.o

objects/findrec.o: sources/findrec.cpp 
	g++ -std=c++11 -o objects/findrec.o -c sources/findrec.cpp

objects/upload.o: sources/upload.cpp 
	g++ -std=c++11 -o objects/upload.o -c sources/upload.cpp

objects/btree.o: sources/btree.cpp headers/btree.hpp headers/util.hpp
	g++ -std=c++11 -o objects/btree.o -c sources/btree.cpp

objects/records.o: sources/records.cpp headers/records.hpp
	g++ -std=c++11 -o objects/records.o -c sources/records.cpp

objects/util.o: sources/util.cpp headers/util.hpp
	g++ -std=c++11 -o objects/util.o -c sources/util.cpp

clean:
	rm -f objects/*.o
	rm -f datafiles/*
	rm -f upload
	rm -f findrec
