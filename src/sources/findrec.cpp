#include "../headers/util.hpp"


int main(int argc, char const *argv[])
{
	
	int fd = open("datafiles/datafile", O_RDWR);
	int fo = open("datafiles/overflow", O_RDWR);
	find_record_datafile(stoi(argv[1]), fd, fo);
	return 0;
}
