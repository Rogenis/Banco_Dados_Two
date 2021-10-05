#include "../headers/btree.hpp"
// Verifica se o campo lido esta vazio ou se comeca com aspas
inline bool field_is_empty(string field){
	return field.empty() || field == "NULL" || field == "\"\"" || field[0] != '\"';
}

// Retorna a string s sem as aspas
string remove_quotes(string s){
	return s.substr(1, s.size()-2);
}

// Insere um registro no arquivo de dados
block_addr insert_record_datafile(record* newRecord, int outputFile, int overflowFile){

	long keyLocation = (newRecord->id % HASHSIZE);
	long blocksPerBucket = 2;
	long AnchorBlock = keyLocation*(sizeof(frame)*blocksPerBucket);
	long offset, offsetAux;

	block_addr returnValue;

	// frame é um vetor de char com 4096 bytes para leitura e escrita
	frame newBlock;																
	memset((void*) newBlock, 0 , sizeof(frame));

	// block é uma struct menor que 4096, pois é fixo então aponta para frame
	block* blockOfRecords = (block*) newBlock;									

	short blockFactor = 7;

	// Muda ponteiro de I/O para inicio do primeiro bloco do bucket
	lseek(outputFile, AnchorBlock, SEEK_SET);									
	read(outputFile, newBlock, sizeof(frame));
										
	// Carrega primeiro bloco para memória
	if(blockOfRecords->nr_records > 0){// Entra se houver registro no bloco

		// Entra se o primeiro bloco NÃO estiver cheio
		if(blockOfRecords->nr_records < blockFactor){					

			memcpy(&blockOfRecords->records[blockOfRecords->nr_records], (void*) newRecord, sizeof(record));

			blockOfRecords->nr_records++;
			// Escreve o primeiro bloco e armazena a posição de inicio de escrita
			offset = lseek(outputFile, 0, SEEK_CUR); 
			write(outputFile, newBlock, sizeof(frame));

			returnValue.addr = offset;

			// Sinaliza que o registro está no arquivo de dados
			returnValue.typeaddr = ADDR_DATAF;

			return returnValue;

		}else{// Entra se o primeiro bloco estiver cheio

			// Carrega segundo bloco para memória e guarda seu endereço
			offsetAux = lseek(outputFile, 0, SEEK_CUR);
			read(outputFile, newBlock, sizeof(frame));				

			// Entra se o segundo bloco NÃO estiver cheio
			if(blockOfRecords->nr_records < blockFactor){

				memcpy(&blockOfRecords->records[blockOfRecords->nr_records], (void*) newRecord, sizeof(record));

				blockOfRecords->nr_records++;

				offset = lseek(outputFile, offsetAux, SEEK_SET); 
				write(outputFile, newBlock, sizeof(frame));

				returnValue.addr = offset;

				// Sinaliza que o registro está no arquivo de dados
				returnValue.typeaddr = ADDR_DATAF;

				return returnValue;


			}else{// Registro entrará para o arquivo de overflow, pois bucket está cheio
				
				frame writeBuffer, readBuffer;
				record_overflow* newOverflowRegister = (record_overflow*) writeBuffer;			

				memcpy(&newOverflowRegister->records, (void*) newRecord, sizeof(record));

				// -1 indica que não há outro registro do bucket depois deste no arquivo de overflow
				newOverflowRegister->next_record_addr = -1;

				// Entra se ainda não tiver registro do bucket no arquivo de overflow
				if(blockOfRecords->overflow_off_t == -1){

					// Move ponteiro de I/O para final do arquivo de overflow
					lseek(overflowFile, 0, SEEK_END);
					offset = lseek(outputFile, 0, SEEK_CUR); 
					write(overflowFile, writeBuffer,sizeof(record_overflow));	

					// Muda o valor do ponteiro do ultimo bloco do bucket para o registro no arquivo de overflow
					blockOfRecords->overflow_off_t = offset;

					lseek(outputFile, offsetAux, SEEK_SET);
					write(outputFile, newBlock, sizeof(frame));

					returnValue.addr = offset;

					// Sinaliza que o registro está no arquivo de overflow
					returnValue.typeaddr = ADDR_OVER;

					return returnValue;



				}else{// Entra caso já tenha algum registro do bucket no arquivo de overflow

					// Cria ponteiro para leitura e manipulação do registro em memória
					record_overflow* overflowRecord = (record_overflow*) readBuffer;

					lseek(overflowFile, blockOfRecords->overflow_off_t,	SEEK_SET);

					offset = lseek(outputFile, 0, SEEK_CUR); 
					read(overflowFile, readBuffer, sizeof(record_overflow));

					// Verifica se o registro de overflow é o último do encadeamento, se não for
					while( overflowRecord->next_record_addr != -1 ){

						// Move ponteiro de I/O para o próximo registro encadeado
						lseek(overflowFile, overflowRecord->next_record_addr, SEEK_SET);

						// Carrega apenas o registro para memória e guarda seu offset
						offset = lseek(outputFile, 0, SEEK_CUR);  
						read(overflowFile, readBuffer,	sizeof(record_overflow));

					}

					lseek(overflowFile, 0, SEEK_END);
					offsetAux = lseek(outputFile, 0, SEEK_CUR); 
					write(overflowFile, writeBuffer, sizeof(record_overflow));

					// Atualiza o offset do agora penúltimo registro do bucket para apontar para o registro recém colocado
					overflowRecord->next_record_addr = offsetAux;

					// Move o ponteiro de I/O para o local do penúltimo registro
					lseek(overflowFile, offset, SEEK_SET);
					write(overflowFile, readBuffer, sizeof(record_overflow));

					// Endereço do registro recém colocado no arquivo de overflow
					returnValue.addr = offsetAux;								

					// Sinaliza que o registro está no arquivo de overflow
					returnValue.typeaddr = ADDR_OVER;		
				
					return returnValue;

				}

			}

		}

	}else{// Entra caso não haja nenhum registro no bloco

		frame anotherBlockOfRegisters;
		memset((void*) anotherBlockOfRegisters, 0, sizeof(frame));
		blockOfRecords = (block*) anotherBlockOfRegisters;

		// Incrementa o número de registros
		blockOfRecords->nr_records++;									
		// -1 indica que não há offset no arquivo de overflow
		blockOfRecords->overflow_off_t = -1;
		memcpy(&blockOfRecords->records[0], (void*)newRecord, sizeof(record));

		frame nextBlockOfRegisters;
		memset((void*) nextBlockOfRegisters, 0, sizeof(block));
		blockOfRecords = (block*) nextBlockOfRegisters;
		blockOfRecords->overflow_off_t = -1;

		// Muda ponteiro de I/O para inicio do primeiro bloco do bucket
		offset =  lseek(outputFile, AnchorBlock, SEEK_SET);
		write(outputFile, anotherBlockOfRegisters, sizeof(frame));
		write(outputFile, nextBlockOfRegisters, sizeof(frame));
		returnValue.addr = offset;

		// Sinaliza que o registro está no arquivo de dados
		returnValue.typeaddr = ADDR_DATAF;

		return returnValue;

	}

	returnValue.typeaddr = -1;

	return returnValue;

}

int find_record_datafile(int inputId, int outputFile, int overflowFile){

	long keyLocation = (inputId % HASHSIZE);
	long blocksPerBucket = 2;
	long blockAdress = keyLocation*(sizeof(frame)*blocksPerBucket);
	int countReadBlocks = 0;
	long totalBlocks = 0;
	long aux = 0;

	frame buffer, bufferAux;
	memset((void*) buffer, 0, sizeof(frame));
	memset((void*) bufferAux, 0, sizeof(frame));

	int i = 0, j = 0;
	int blockFactor = 7;

	block* blockOfRecords = (block*) buffer;
	record_overflow* overflowRecord = (record_overflow*) bufferAux;

	lseek(outputFile, blockAdress, SEEK_SET);
	read(outputFile, buffer, sizeof(frame));
	countReadBlocks++;

	// Procura nos dois blocos do bucket
	while(j < blocksPerBucket){													

		// Procura no bucket o registro pelo id até achar
		while(i < blockFactor){													

			// Compara ids dos registros do bloco carregado para memória
			if(blockOfRecords->records[i].id == inputId){						

				memcpy((void*) &bufferAux, (void*) buffer, sizeof(record));
				cout << "Número de blocos lidos: " << countReadBlocks << endl;
				aux = lseek(outputFile, 0, SEEK_END);
				totalBlocks = aux/4096;
				aux = lseek(overflowFile, 0, SEEK_END);
				totalBlocks = totalBlocks + (aux/4096);
				cout << "Número total de blocos: " << totalBlocks << endl;
				print_record(&blockOfRecords->records[i]);

				return 1;

			}

			i++;

		}
		
		// Carrega outro bloco para memória, visto que read move ponteiro de I/O
		read(outputFile, buffer, sizeof(frame));
		countReadBlocks++;								
		j++;																	// não precisa ter outro seek
		i = 0;

	}

	if(blockOfRecords->overflow_off_t != -1){					

		// Se chegou aqui, então o registro não estava nos blocos, então verifica se																		
		// o registro está no arquivo de overflow
		// Move ponteiro de I/O para o registro de overflow apontado pelo último bloco do bucket
		lseek(overflowFile, blockOfRecords->overflow_off_t, SEEK_SET);

		// Carrega apenas o registro desejado e não o bloco
		read(overflowFile, buffer, sizeof(record_overflow));
		countReadBlocks++;

		// Enquanto o registro não tiver o id desejado e houver registros encadeados
		while((overflowRecord->records.id != inputId) 							
				&& overflowRecord->next_record_addr != -1){

			// Move ponteiro de I/O para o próximo registro encadeado
			lseek(overflowFile, overflowRecord->next_record_addr, SEEK_SET);

			// Carrega apenas o registro para memória
			read(overflowFile, buffer, sizeof(record_overflow));
			countReadBlocks++;

		}

		// Se chegou aqui, então o registro foi encontrado ou não há mais registros encadeados
		if(overflowRecord->records.id == inputId){

			memcpy((void*) &bufferAux, (void*) buffer, sizeof(record));

			cout << "Número de blocos lidos: " << countReadBlocks << endl;
			aux = lseek(outputFile, 0, SEEK_END);
			totalBlocks = aux/4096;
			aux = lseek(overflowFile, 0, SEEK_END);
			totalBlocks = totalBlocks + (aux/4096);
			cout << "Número total de blocos: " << totalBlocks << endl;
			print_record(&overflowRecord->records);

			return 1;

		}

	}

	return 1;

}

// Cria um arquivo de dados vazio
int create_datafile(){

	int datafile = open("datafiles/datafile", O_CREAT | O_TRUNC | O_RDWR, 00700);

//if(datafile < 0 || alocar_hash(datafile) < 0)
	if(datafile < 0)
		return -1;

	return close(datafile);

}

// Abre o arquivo de dados usando descritor de arquivo
int open_datafile(){
	return open("datafiles/datafile", O_RDWR);
}

// Cria um arquivo de overflow vazio
int create_overflowfile(){

	//int fo = open("datafiles/overflowfile", O_CREAT | O_TRUNC, 00700);
	int fo = open("datafiles/overflow", O_CREAT | O_TRUNC, 00700);

	if(fo < 0)
		return -1;

	return close(fo);

}

// Abre o arquivo de overflow usando descritor de arquivo
int open_overflowfile(){
	return open("datafiles/overflow", O_RDWR);
}

// Le o arquivo de entrada e insere no arquivo de dados
int upload_file(string filePath){

	string line, field;
	record new_record;

	size_t slen;

	regex semicolon(";");
	regex_token_iterator<string::iterator> end;

	// Arquivo de entrada
	ifstream inputFile;
	inputFile.open(filePath);

	if(!inputFile.is_open()){
		cout << "ERROR: Arquivo nao encontrado!" << endl;
		return -1;
	}

	// Arquivo de dados 
	if(create_datafile() < 0){
		cout << "O arquivo de dados nao pode ser criado." << endl;
		return -1;
	}
	
	int datafile = open_datafile();
	if(datafile < 0){
		cout << "ERROR: datafile nao aberto." << endl;
		return -1;
	}

	// Arquivo de overflow
	if(create_overflowfile() < 0){
		cout << "O arquivo de dados nao pode ser criado." << endl;
		return -1;
	}
	
	int overflowfile = open_overflowfile();
	if(overflowfile < 0){
		cout << "ERROR: overflowfile nao aberto." << endl;
		return -1;
	}

	//Lendo os registros do arquivo de entrada. 
	while(getline(inputFile, line)){
		
		// Apaga o \n
		line.pop_back(); 
		

		// Cria um iterador com o ; como separador
		regex_token_iterator<string::iterator> next(line.begin(), line.end(), semicolon, -1);
		
		// Lendo o ID
		field = *next++;
		if(field_is_empty(field) || field.back() != '\"'){
			cout << "ERROR: ID - " << field << " invalido!" << endl;
			continue;

		}else{

			field = remove_quotes(field);
			new_record.id = stoi(field, nullptr, 10);
		}

		cout << "Lendo ID: " << new_record.id << "..." << endl;

		// Lendo o titulo
		field = *next++;
		if(field_is_empty(field)){
			cout << "Warning: Titulo - " << field << " invalido!" << endl;
			new_record.title[0] = 0;

		}else{
			
			// Concatena os iteradores ate que o campo seja fechado com aspas
			while(field.back() != '\"'){
 
                if(next == end){

                	// Se a linha acabou, ele busca o final do campo nos iteradores da proxima linha
                    if(getline(inputFile, line)){
                        regex_token_iterator<string::iterator> new_iterator(line.begin(), line.end(), semicolon, -1);
                        next = new_iterator;
                         
                    }else{

                    	// Se o arquivo for esvaziado, significa que o campo atual nao foi fechado 
                    	// com aspas e por conta disso nao da para saber onde ele foi fechado
						cout << "aquiiii 1\n";
                        cout << "ERROR: ID "<< new_record.id  << " : Titulo - O arquivo chegou ao fim sem concluir a insercao" << endl;
               			return -1;
                    }
 
                }else{

                	// Concatena os iteradores com o campo atual ate que apareca as aspas no final
                    field += *next++;   
                }
            }

			field = remove_quotes(field);
			slen = field.copy(new_record.title, 299, 0);
			new_record.title[slen] = 0;	
		}

		// Ano
		field = *next++;
		if(field_is_empty(field) || field.back() != '\"'){
			cout << "Warning: Ano - " << field << " invalido!" << endl;
			new_record.year = 0;

		}else{

			field = remove_quotes(field);
			new_record.year = stoi(field, nullptr, 10);
		}

		// Autores
		field = *next++;
		if(field_is_empty(field)){
			cout << "Warning: Autores - " << field << " invalido!" << endl;
			new_record.autors[0] = 0;

		}else{

			// Concatena os iteradores ate que o campo seja fechado com aspas
			while(field.back() != '\"'){
 
                if(next == end){

                	// Se a linha acabou, ele busca o final do campo nos iteradores da proxima linha
                    if(getline(inputFile, line)){
                        regex_token_iterator<string::iterator> new_iterator(line.begin(), line.end(), semicolon, -1);
                        next = new_iterator;
                         
                    }else{

                    	// Se o arquivo for esvaziado, significa que o campo atual nao foi fechado 
                    	// com aspas e por conta disso nao da para saber onde ele foi fechado
						cout << "aquiiii 222\n";
                        cout <<"ERROR: ID "<< new_record.id  << " : Autores - O arquivo chegou ao fim sem concluir a insercao" << endl;
               			return -1;
                    }
 
                }else{

                	// Concatena os iteradores com o campo atual ate que apareca as aspas no final
                    field += *next++;   
                }
            }

			
			field = remove_quotes(field);
			slen = field.copy(new_record.autors, 99, 0);
			new_record.autors[slen] = 0;	
		}

		// Citacoes
		field = *next++;
		if(field_is_empty(field) || field.back() != '\"'){
			cout << "Warning: Citacoes - " << field << " invalido!" << endl;
			new_record.mention = 0;

		}else{

			field = remove_quotes(field);
			new_record.mention = stoi(field, nullptr, 10);
		}

		// Timestamp
		field = *next++;
		if(field_is_empty(field) || field.back() != '\"'){
			cout << "Warning: Timestamp - " << field << " invalido!" << endl;
			new_record.timestamp = str_to_date("0000-00-00 00:00:00");

		}else{

			field = remove_quotes(field);
			new_record.timestamp = str_to_date(field);

		}

		// Snippet
		field = *next++;
		if(field_is_empty(field)){
			cout << "Warning: Snippet - " << field << " invalido!" << endl;
			new_record.snippet[0] = 0;

		}else{

			// Concatena os iteradores ate que o campo seja fechado com aspas
			while(field.back() != '\"'){
 
                if(next == end){

                	// Se a linha acabou, ele busca o final do campo nos iteradores da proxima linha
                    if(getline(inputFile, line)){
                        regex_token_iterator<string::iterator> new_iterator(line.begin(), line.end(), semicolon, -1);
                        next = new_iterator;
                         
                    }else{

                    	// Se o arquivo for esvaziado, significa que o campo atual nao foi fechado 
                    	// com aspas e por conta disso nao da para saber onde ele foi fechado
                        cout << "ERROR: ID "<< new_record.id  << " - Snippet O arquivo chegou ao fim sem concluir a insercao" << endl;
               			return -1;
                    }
 
                }else{

                	// Concatena os iteradores com o campo atual ate que apareca as aspas no final
                    field += *next++;   
                }
            }
             
			field = remove_quotes(field);
			slen = field.copy(new_record.snippet, 99, 0);
			new_record.snippet[slen] = 0;	
		}

		// Tombstone
		new_record.tombstone = 0;

		// Inserindo no arquivo de dados
		block_addr addr_id = insert_record_datafile(&new_record, datafile, overflowfile);

		// Indexando as chaves
		pair_id key;
		key.key = new_record.id;
		key.ptr = addr_id;

	}

	close(datafile);
	close(overflowfile);
	//close(index_pfile);
	inputFile.close();

	return 0;
}
