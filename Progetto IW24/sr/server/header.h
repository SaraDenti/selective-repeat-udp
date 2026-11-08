#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "../shared/packet.h"

struct lista_buffer;

//Gestione multithreading
typedef struct _thread_struct{
    struct sockaddr_in *address; //indirizzo del socket destinazione
    uint32_t thread_index;
    nseq_t rcv_base;
    nseq_t rcv_limit_window;
    nseq_t send_base;
    nseq_t send_limit_window;
    nseq_t next_seqnum;
    struct lista_buffer *rcv_list;
    struct lista_buffer *buffer_list;
    char array_ack[WINDOW];
    struct timeval *timeout_adattivo;
    double estimated_RTT; //Stima dell RTT iniziale
    double dev_RTT; //Variazione dell RTT
    pthread_t tid;
} thread_struct;

typedef struct _thread_node{
    uint32_t index;
    struct _thread_node *next;
} thread_node;

struct _thread_list{
    thread_node* first;
    thread_node*last;
};

extern pthread_t main_tid;

extern struct _thread_list thread_list; //E' la lista degli indici di thread_array vuoti
extern thread_struct *thread_array[WORKER_THREADS_NUM];

extern int server_socket; //ID del socket del server
extern struct sockaddr_in client_address; //Struttura su cui si memorizza l'indirizzo del client da cui si riceve un pacchetto
extern socklen_t client_addr_len;

extern int sem_thread_list;

extern struct timeval timeout_fisso; 

extern bool opened; 
void error(char *s);

void invia_pacchetto(packet_t *packet);
void ricevi_pacchetto(packet_t *packet);
bool indirizzo_uguale(struct sockaddr_in *address1, struct sockaddr_in *address2);

bool in_window(nseq_t nseq, nseq_t base, nseq_t limit);

void elimina_connessione(thread_struct *t_struct);

void create_thread_node(uint32_t n);
thread_struct *crea_thread_struct(struct sockaddr_in *_address, uint32_t _thread_index, nseq_t _rcv_base, nseq_t _send_base);
void delete_thread_struct(uint32_t thread_index);