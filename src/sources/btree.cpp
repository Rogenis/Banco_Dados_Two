// b-tree

#include "../headers/btree.hpp"


int total_nodes = 0;

// Cria um arquivo de indices contendo apenas o cabeçalho e a raiz 
int create_index_tree(){

	int fd = open("indexes/primary_tree", O_WRONLY | O_CREAT | O_TRUNC, 00700);

	if(fd < 0){
		cout << "ERROR: " << "indexes/primary_tree" << " nao pode ser aberto" << endl;
		return -1;
	}

	// Instanciando a nova arvore
	frame block;
	btree* header_tree = (btree*) block;

	// Setando configuracoes iniciais da arvore
	header_tree->height = 1;
	header_tree->ptr_root.addr = 4096;	// Segundo bloco do arquivo

	// Escrevendo o bloco no disco (Cabecalho)
	if(write(fd, block, PAGE_SIZE) < 0){
		cout << "ERROR: arquivo de indices nao pode ser criado" << endl;
		return -1;
	}
	

	// Instanciando a raiz
	primary_tree_node* root = (primary_tree_node*) block;

	// Setando configuracoes iniciais da raiz
	root->nr_regstr = 0;
	root->last_ptr.addr = 0;
	root->last_ptr.typeaddr = ADDR_LEAF;

	// Escrevendo a raiz no disco
	if(write(fd, block, PAGE_SIZE) < 0){
		cout << "ERROR: arquivo de indices nao pode ser criado" << endl;
		return -1;
	}

	if(close(fd) < 0){
		cout << "ERROR: " << "indexes/primary_tree" << " nao foi fechado corretamente" << endl;
		return -1;
	}
	
	return 0;
}

// Abre o arquivo de indice
int open_index_tree(){
	return open("indexes/primary_tree", O_RDWR);
}

//Retorna o indice do proximo nodo da busca .
int get_position(int id, pair_id* indexes, int nr_regstr){

	int i = 0;
	int j = nr_regstr -1;
	int mid = 0;
		
	while(i <= j){
		
		mid = (i + j)/ 2;
		
		if(id < indexes[mid].key){
			j = mid -1;
			
		}else if(id > indexes[mid].key){
			i = mid+1;
			
		}else{
			return mid+1;			
		}
		
	}

	if(id > indexes[mid].key){
		return mid +1;
	}

	return mid;

}

// Ordena o vetor de indices
void sort(pair_id* indexes, int nr_regstr) {
  
	int i;
	pair_id pivot;
	i = nr_regstr - 2;
	pivot = indexes[nr_regstr -1];

	while(i >=0 && pivot.key < indexes[i].key){
			
			indexes[i+1] = indexes[i];
			i--;
			
	}

	indexes[i+1]= pivot;

}

// Quebra uma folha em duas e retorna uma referencia para o nodo criado
pair_id split_in_leaf(pair_id new_record, int fd_btree, long node_cur_addr, primary_tree_node* node_tree){
	
	//elemento que guardara o elemento que subira
	pair_id new_reference;
	
	// Guardando o M 
	int m = BTREE_ID_ORDER * 2;

	// Instanciando um vetor que comporte todos os elementos do nodo mais o elemento extra
	pair_id full_content[m+1];

	// Copiando todos os elementos do no atual para o novo no
	memcpy(full_content, node_tree->regstr, m*sizeof(pair_id));

	// Colocando o elemento no nodo
	full_content[m] = new_record;
	sort(full_content, m+1);

	// Criando o novo no 
	frame new_block;
	primary_tree_node* new_node = (primary_tree_node*) new_block;

	// Setando configuracoes iniciais do novo nodo
	new_node->nr_regstr = BTREE_ID_ORDER + 1;
	new_node->last_ptr = node_tree->last_ptr;

	// Setando novas configuracoes para a folha sendo quebrada
	node_tree->nr_regstr = BTREE_ID_ORDER;

	// Passando a primeira metade dos elementos para antiga folha
	memcpy(node_tree->regstr, full_content, BTREE_ID_ORDER*sizeof(pair_id));

	// Passando a segunda metado dos elementos para a nova folha
	memcpy(new_node->regstr, &full_content[BTREE_ID_ORDER], (BTREE_ID_ORDER + 1)*sizeof(pair_id));

	// Colocando o novo nodo no disco
	long new_addr = lseek(fd_btree, 0, SEEK_END);

	if(new_addr < 0 || write(fd_btree, new_block, PAGE_SIZE) < 0){
		cout << "ERROR: falha na criacao da folha"  << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	// Salvando as novas configuracoes do nodo quebrado no disco
	node_tree->last_ptr.addr = new_addr;
	node_tree->last_ptr.typeaddr = ADDR_LEAF;

	// Retornando uma referencia para o novo nodo
	new_reference.ptr = node_tree->last_ptr;
	new_reference.key = full_content[BTREE_ID_ORDER].key;

	// Saltando para o bloco que contem o nodo atual
	if(lseek(fd_btree, node_cur_addr, SEEK_SET) < 0 || write(fd_btree, node_tree, PAGE_SIZE) != PAGE_SIZE){
		cout << "ERROR: falha em atualizar a folha" << endl;
		new_reference.ptr.typeaddr = -1;
		
	}
	
	return new_reference;
}

// Quebra um nodo interno em dois e retorna uma referencia para o nodo criado
pair_id split_in_node(pair_id new_record, int fd_btree, long node_cur_addr, primary_tree_node* node_tree, pair_id new_reference){
	
	// Guardando o M 
	int m = BTREE_ID_ORDER * 2;

	// Instanciando um vetor que comporte todos os elementos do nodo mais o elemento extra
	pair_id full_content[m+1];

	// Copiando todos os elementos do no atual para o novo no
	memcpy(full_content, node_tree->regstr, m*sizeof(pair_id));

	full_content[m] = new_reference;
	sort(full_content, m+1);

	// Criando o novo no 
	frame new_block;
	primary_tree_node* new_node = (primary_tree_node*) new_block;

	// Setando configuracoes iniciais do novo nodo
	new_node->nr_regstr = BTREE_ID_ORDER;
	new_node->last_ptr = node_tree->last_ptr;

	// Setando novas configuracoes o nodo quebrado
	node_tree->nr_regstr = BTREE_ID_ORDER;

	// Passando a primeira metade dos elementos para antiga folha
	memcpy( node_tree->regstr,  full_content, BTREE_ID_ORDER*sizeof(pair_id));

	// Passando a segunda metado dos elementos para a nova folha
	memcpy( new_node->regstr,  &full_content[BTREE_ID_ORDER+1], BTREE_ID_ORDER*sizeof(pair_id));

	// Colocando o novo nodo no disco
	long new_addr = lseek(fd_btree, 0, SEEK_END);

	if(new_addr < 0){
		cout << "ERROR: seek no indice primario no offset " << node_cur_addr << " nao pode ser alcancado." << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	if(write(fd_btree, new_block, PAGE_SIZE) < 0){
		cout << "ERROR: a escrita no offset " << node_cur_addr << " falhou." << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	
	// Saltando para o bloco que contem o nodo atual
	if(lseek(fd_btree, node_cur_addr, SEEK_SET) < 0){
		cout << "ERROR: seek no indice primario no offset " << node_cur_addr << " nao pode ser alcancado." << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	if(write(fd_btree, node_tree, PAGE_SIZE) < 0){
		cout << "ERROR: a escrita no offset " << node_cur_addr << " falhou." << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	// Retornando uma referencia para o novo nodo
	new_reference.ptr.addr = new_addr;
	new_reference.key = full_content[BTREE_ID_ORDER].key;	
	
	return new_reference;
}

// Insere um novo indice no arquivo primario
pair_id insert_in_node(pair_id new_record, int fd_btree, long node_cur_addr, int level){

	frame block;
	pair_id new_reference;
	new_reference.ptr.typeaddr = -2;

	// Saltando para o bloco que contem o nodo atual // Lendo o nodo do arquivo de indices
	if(lseek(fd_btree, node_cur_addr, SEEK_SET) < 0 || read(fd_btree, block, PAGE_SIZE) < 0){
		cout << "ERROR: falha no seek ou leitura no indice primario no endereco " << node_cur_addr << " ." << endl;
		new_reference.ptr.typeaddr = -1;
		return new_reference;
	}

	// Interpretando o bloco lido como um nodo da arvore
	primary_tree_node* node_tree = (primary_tree_node*) block;

	// Verificando se o nodo atual e um nodo folha. Se for, entao insere o novo registro
	if(level == 0){

		// split em folha
		if(node_tree->nr_regstr > BTREE_ID_ORDER * 2){
			return split_in_leaf(new_record, fd_btree, node_cur_addr, node_tree);

		}else{

			// Se ainda ha espaco entao coloca no final da folha e ordena
			node_tree->regstr[node_tree->nr_regstr] = new_record;
			node_tree->nr_regstr++;
			sort(node_tree->regstr, node_tree->nr_regstr);

			// Saltando para o bloco que contem o nodo atual e escrevendo no disco
			if(lseek(fd_btree, node_cur_addr, SEEK_SET) < 0 || write(fd_btree, block, PAGE_SIZE) < 0 ){
				cout << "ERROR: falha em atualizar a folha" << endl;
				new_reference.ptr.typeaddr = -1;
				
			}

			return new_reference;
		}
	}
	
	// Se o nodo atual for um nodo interno da arvore
	
	// Obtendo a posicao do nodo 
	int position = get_position(new_record.key, node_tree->regstr, node_tree->nr_regstr);
	
	// desce um nivel na arvore e recebe uma referencia para algum nodo
	new_reference = insert_in_node(new_record, fd_btree, node_tree->regstr[position].ptr.addr, level - 1);

	// Se o nodo nao foi quebrado entao encerra a insercao
	if(new_reference.ptr.typeaddr < 0){
		return new_reference;
	}

	// Trocando o valor das chaves para consertar a ordem do nodo
		int tmp = node_tree->regstr[position].key;
		node_tree->regstr[position].key = new_reference.key;
		new_reference.key = tmp;

	// split em no interno
	if(node_tree->nr_regstr > BTREE_ID_ORDER * 2){
		
		return split_in_node(new_record, fd_btree, node_cur_addr, node_tree, new_reference);

	}else{
		// Se ainda ha espaco entao ocorre um shift de um elemento para a direita e o elemento e inserido. Obs: '+ sizeof(block_addr)' por conta do ultimo ponteiro
		memcpy(&node_tree->regstr[position+1], &node_tree->regstr[position], sizeof(pair_id)*(position - node_tree->nr_regstr) + sizeof(block_addr));
		
		// Coloca o novo elemento na posicao correta
		node_tree->regstr[position] = new_reference;
		node_tree->nr_regstr++;
		for(int i = 0; i < node_tree->nr_regstr ; i++)
			cout << node_tree->regstr[i].key << " ";

		cout << endl;
		// Saltando para o bloco que contem o nodo atual
		if(lseek(fd_btree, node_cur_addr, SEEK_SET) < 0 || write(fd_btree, node_tree, PAGE_SIZE) < 0){
			cout << "ERROR: seek e write no indice primario no offset " << node_cur_addr << " nao foram concluidos." << endl;
			new_reference.ptr.typeaddr = -1;	
			return new_reference;
		}

		// Setando -2 pra nova referencia para avisar que nao ocorreu split
		new_reference.ptr.typeaddr = -2;
		
	}

	return new_reference;
}

int insert_in_tree(pair_id key, int fd_btree){

	frame block_header;
	btree* tree = (btree*) block_header;

	if(lseek(fd_btree, 0, SEEK_SET) < 0 || read(fd_btree, block_header, PAGE_SIZE) < 0){
		cout << "ERROR: nao foi possivel carregar o cabeçalho da arvore primaria." << endl;
		return -1;
	}

	pair_id node_returned = insert_in_node(key, fd_btree, tree->ptr_root.addr, tree->height - 1);

	// Erro
	if(node_returned.ptr.typeaddr == -1)
		return -1;

	//nao ocorreu split
	if(node_returned.ptr.typeaddr == -2)
		return 0;

	frame block_new_root;
	primary_tree_node* new_root = (primary_tree_node*) block_new_root;

	long new_addr = lseek(fd_btree, 0, SEEK_END);

	// Criando a nova raiz

	new_root->nr_regstr = 1;
	new_root->last_ptr.addr = 1;
	new_root->regstr[0].ptr = tree->ptr_root;
	new_root->regstr[0].key = node_returned.key;
	new_root->regstr[1] = node_returned;

	if( new_addr < 0 || write(fd_btree, block_new_root, PAGE_SIZE) < 0){
		cout << "ERROR: nao foi possivel escrever a nova raiz da arvore primaria." << endl;
		return -1;
	}
	
	// Configurando o cabecalho
	tree->height++;
	tree->ptr_root.addr = new_addr;

	if(lseek(fd_btree, 0, SEEK_SET) < 0 || write(fd_btree, block_header, PAGE_SIZE) < 0){
		cout << "ERROR: nao foi possivel escrever o cabeçalho da arvore primaria." << endl;
		return -1;
	}

	return 0;

}