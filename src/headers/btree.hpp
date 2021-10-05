#ifndef BTREE
#define BTREE

#include "util.hpp"

#ifndef BTREE_ID_ORDER

//PTR_SIZE definido em headers/util.hpp

#define KEY_ID_SIZE sizeof(int)
#define BTREE_ID_ORDER 4 //(PAGE_SIZE - PTR_SIZE)/(2*PTR_SIZE + 2*KEY_ID_SIZE)

#endif

#ifndef BTREE_T_ORDER

#define KEY_T_SIZE 300
#define BTREE_T_ORDER (PAGE_SIZE - PTR_SIZE)/(2*PTR_SIZE + 2*KEY_T_SIZE)

#endif


// Arquivo de indice primario - ID
typedef struct pair_id{

	block_addr ptr;
	int key;
	
}pair_id;

typedef struct {

	int nr_regstr;
	pair_id regstr[BTREE_ID_ORDER*2];
	block_addr last_ptr;
	
}primary_tree_node;


typedef struct {
	
	int height;
	block_addr ptr_root;

}btree;

//Arquivo de indice secundario - Titulo
typedef struct {

	block_addr ptr;
	char key[KEY_T_SIZE];
	
}pair_title;

typedef struct {
	
	int nr_regstr;
	
	pair_title regstr[BTREE_T_ORDER*2];
	
	block_addr last_ptr;

}secondary_tree_node;


// Cria um arquivo de indices contendo apenas o cabeçalho e a raiz 
int create_index_tree();

// Abre o arquivo de indice
int open_index_tree();

// quebra um nodo folha em dois
pair_id split_in_leaf(pair_id new_record, int fd_btree, long node_cur_addr, primary_tree_node* node_tree);

// quebra um nodo interno em dois
pair_id split_in_node(pair_id new_record, int fd_btree, long node_cur_addr, primary_tree_node* node_tree, pair_id new_reference);

// Insere uma chave no nodo da arvore
pair_id insert_in_node(pair_id new_record, int fd_btree, long node_cur_addr, int level);

// Insere uma chave na arvore
int insert_in_tree(pair_id key, int fd_btree);

#endif
