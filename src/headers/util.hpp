#ifndef PARSERDEF
#define PARSERDEF

#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <stack>

#include "records.hpp"

#define PAGE_SIZE 4096
#define HASHSIZE 256000

#define	ADDR_DATAF 0
#define ADDR_OVER  1
#define ADDR_INNER 2
#define ADDR_LEAF  3

using namespace std;

typedef char frame[PAGE_SIZE];

// Estrutura de um endereco de registro ou bloco - 8 bytes
typedef struct {

	// indica se o endereco e de um arquivo de dados(0), arquivo de overflow(1), no interno (2) ou uma folha (3)
	int typeaddr;

	// addr no arquivo
	long addr;
	
}block_addr;

// Estrutura de um block do bucket - Utilizado apenas para interpretacao de um bloco lido 
typedef struct block{

	short nr_records;
	record records[7];
	long overflow_off_t;

}block;

// Estrutura de um registro dentro do arquivo de overflow
typedef struct record_overflow{

	// Registro 
	record records;

	// Ponteiro pro proximo registro
	long next_record_addr;

}record_overflow;

// Tamanho de um ponteiro
#define PTR_SIZE sizeof(block_addr)

// Retorna a string s sem as aspas
string remove_quotes(string s);

// Cria um arquivo de dados vazio
int create_datafile();

// Abre o arquivo de dados usando descritor de arquivo
int open_datafile();

// Cria um arquivo de overflow vazio
int create_overflowfile();

// Abre o arquivo de overflow usando descritor de arquivo
int open_overflowfile();

// insere o registro no arquivo de dados
block_addr insert_record_datafile(record* newRecord, int outputFile, int overflowFile);

// Busca um registro no arquivo de dados
int find_record_datafile(int inputId, int outputFile, int overflowFile);

// Le o arquivo de entrada e insere no arquivo de dados
int upload_file(string inputFile);

#endif 
