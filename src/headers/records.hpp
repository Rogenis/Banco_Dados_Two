#ifndef RECORDS
#define RECORDS

#include<iostream>
#include<string>
#include<regex>

using namespace std;

// Estrutura de um timestamp. Veja que ele gasta somente 8 bytes contra os 16 gravando a string literal
typedef struct{
	
	char day;
	char month;
	short year;

	char sec;
	char min;
	char hour;

}date;

// Estrutura de um registro - 524 bytes 
typedef struct{

	int id;
	char title[300];
	int year;
	char autors[100];
	int mention;
	date timestamp;
	char snippet[100];
	int tombstone;

}record;

// Converte uma variavel date para string
string date_to_str(date d);

// a funcao retorna um timestamp 
date str_to_date(string timestamp);

// Imprime os dados de um registro na saida padrao
void print_record(record* regster);

#endif
