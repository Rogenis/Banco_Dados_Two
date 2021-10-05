#include "../headers/util.hpp"

void usage(){

	cout << "Modo de Uso: ./upload file_path"  << endl;
	cout << "Ex: ./upload arquivo.cvs" << endl;

	return;
}


int main(int argc, char const *argv[]){
	

	if(argc != 2){
		usage();
		return -1;
	}	


	if(upload_file(argv[1])){
		return -1;
	}

	

	return 0;
}
