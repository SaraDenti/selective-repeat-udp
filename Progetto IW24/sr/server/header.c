#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <signal.h>
#include <math.h>
#include <errno.h>

#include <sys/sem.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "buffer_server.h"
#include "header.h"
#include "timer_server.h"

#include "../shared/config.h"
#include "../shared/packet.h"

/**********************************************************************************************************
                                            FUNZIONE D'ERRORE
**********************************************************************************************************/
void error(char *s){
    perror(s);
    fflush(stderr);
    if(opened == true){ 
        while(close(server_socket) == -1){
            if(errno == EINTR){
                errno = 0;
            }else{
                fprintf(stderr, "[SERVER] Errore della funzione \"close\" in \"error\"\n");
                fflush(stderr);
                break;
            }
        }
    }
    exit(EXIT_FAILURE);
}

/**********************************************************************************************************
                                            GESTIONE PACCHETTI
**********************************************************************************************************/

// -------------------------------------------- INVIO PACCHETTI --------------------------------------------

static float loss_probability = 0.05; //5% di probabilità di perdita

//Tale funzione ritorna true se il pacchetto viene perso, false altrimenti
static bool simulate_loss(float p){
    return ((float)rand() / RAND_MAX) < p;
}

void invia_pacchetto(packet_t *packet){
    if(simulate_loss(loss_probability) == false){
        if (packet->header.ack_flag == 0){
            if (pthread_self() == main_tid){
                printf("[MAIN THREAD] Inviando un pacchetto di %d bytes (n_seq = %d)\n", ntohl(packet->header.len), ntohl(packet->header.n_seq));
                fflush(stdout);
            }else{
                printf("[THREAD %ld] Inviando un pacchetto di %d bytes (n_seq = %d)\n", pthread_self(), ntohl(packet->header.len), ntohl(packet->header.n_seq));
                fflush(stdout);
            }
        }
        else{
            if (pthread_self() == main_tid){
                printf("[MAIN THREAD] Inviando un pacchetto di ACK di %d bytes (ack = %d)\n", ntohl(packet->header.len), ntohl(packet->header.ack));
                fflush(stdout);
            }else{
                printf("[THREAD %ld] Inviando un pacchetto di ACK di %d bytes (ack = %d)\n", pthread_self(), ntohl(packet->header.len), ntohl(packet->header.ack));
                fflush(stdout);
            }
        }
        ssize_t byte_scritti = 0;
        ssize_t ret;
        while(byte_scritti < (ssize_t)ntohl(packet->header.len)){
            ret = sendto(server_socket, &packet[byte_scritti], ntohl(packet->header.len) - byte_scritti, MSG_CONFIRM, (struct sockaddr*) &packet->header.address, sizeof(packet->header.address));
            if(ret == -1){
                if(errno == EINTR) errno = 0;
                else if((errno == EHOSTUNREACH) || (errno == ENETUNREACH) || (errno == EADDRNOTAVAIL)){
                    fprintf(stderr, "[THREAD %ld] Connessione al client non riuscita\n", pthread_self());
                    fflush(stdout);

                    if (thread_array[ntohl(packet->header.index)] != NULL){
                        elimina_connessione(thread_array[ntohl(packet->header.index)]);
                        pthread_kill(thread_array[ntohl(packet->header.index)]->tid, SIGTERM);
                        delete_thread_struct(ntohl(packet->header.index));
                    }
                    free(packet);
                }
                else{
                    error("[SERVER] Errore della funzione \"sendto()\" in \"invia_pacchetto\"");
                }
            }
            else{
                byte_scritti += ret;
            }
        }
    }
    else{
        if (packet->header.ack_flag == 0){
            printf("[THREAD %ld] Pacchetto perso (n_seq = %d)\n", pthread_self(), ntohl(packet->header.n_seq));
            fflush(stdout);
        }
        else{
            printf("[THREAD %ld] Pacchetto perso (ack = %d)\n", pthread_self(), ntohl(packet->header.ack));
            fflush(stdout);
        }
    }
}

// ------------------------------------------ RICEZIONE PACCHETTI ------------------------------------------

void ricevi_pacchetto(packet_t *packet){
    memset(packet, '\0', MAX_PACKET_LEN);

    ssize_t byte_letti = 0;
    ssize_t ret;
    while(byte_letti < MAX_PACKET_LEN){
        ret = recvfrom(server_socket, packet + byte_letti, MAX_PACKET_LEN - byte_letti, 0, (struct sockaddr*)&client_address, &client_addr_len);
        if(ret == -1){
            if(errno == EINTR) errno = 0;
            else error("[SERVER] Errore della funzione \"recvfrom()\" in \"ricevi_pacchetto\"");
        }
        else{
            byte_letti += ret;
            if ((long unsigned int) byte_letti >= sizeof(packet_header_t)){
                if ((long unsigned int)byte_letti == ntohl(packet->header.len)){
                    break;
                } 
            }
        }
    }
}

bool indirizzo_uguale(struct sockaddr_in *address1, struct sockaddr_in *address2){
    return (address1->sin_port == address2->sin_port) && (address1->sin_addr.s_addr == address2->sin_addr.s_addr);
}

/**********************************************************************************************************
                                            FINESTRA DI RICEZIONE/TRASMISSIONE
**********************************************************************************************************/

//Questa funzione verifica se nseq appartiene alla finestra (di ricezione o di trasmissione)
bool in_window(nseq_t nseq, nseq_t base, nseq_t limit){
    if (base <= limit){
        if ((nseq >= base) && (nseq<=limit)){
            return true;
        }
    }
    else{
        if (((nseq >= base) && (nseq < MAX_MSG_CLUSTER)) || (nseq<=limit)){
            return true;
        }
    }
    return false;
}

/**********************************************************************************************************
                                                MULTITHREADING
**********************************************************************************************************/

void elimina_connessione(thread_struct *t_struct){
    timer *curr = timer_list.first;
    timer *old = NULL;
    while (curr != NULL){
        if (indirizzo_uguale(&curr->packet->header.address, t_struct->address) && (ntohl(curr->packet->header.index) == t_struct->thread_index)){
            old = curr;
            curr = curr->next;
        
            handle_ack(ntohl(old->packet->header.n_seq), t_struct, false);       
        }
        else{
            old = curr;
            curr = curr->next;
        }
    }
}

void create_thread_node(uint32_t n){
    //Blocchiamo il segnale SIGALRM
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigprocmask(SIG_BLOCK, &set, NULL);

    //Crea il nuovo nodo
    thread_node *node = malloc(sizeof(thread_node));
    if (node == NULL){
        error("[SERVER] Allocazione memoria fallita in \"create_thread_node\"");
    }
    node->index = n;
    node->next = NULL;

    //Richiede un token a sem_thread_list per ottenere l'accesso esclusivo a thread_list
    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    while (semop(sem_thread_list, &buff, 1) == -1){
        if (errno == EINTR) errno = 0;
        else error("[SERVER] Errore della funzione \"semop\" in \"create_thread_node\"");
    }

    //Inserisce il nuovo nodo in thread_list
    if (thread_list.first == NULL){ 
        //E' l'unico elemento di thread_list
        thread_list.first = node;
        thread_list.last = node;
    }
    else{
        //Il nuovo nodo viene posto alla fine della lista
        thread_list.last->next = node;
        thread_list.last = node;
    }

    buff.sem_op = +1;
    while (semop(sem_thread_list, &buff, 1) == -1){
        if (errno == EINTR) errno = 0;
        else error("[SERVER] Errore della funzione \"semop\"(1) in \"create_thread_node\"");
    }

    //Sblocchiamo il segnale SIGALRM
    sigprocmask(SIG_UNBLOCK, &set, NULL);
}

//Inizializza una struttura thread_struct e i suoi elementi eccezion fatta per il TID
thread_struct *crea_thread_struct(struct sockaddr_in *_address, uint32_t _thread_index, nseq_t _rcv_base, nseq_t _send_base){
    thread_struct *new_struct = malloc(sizeof(thread_struct));
    if (new_struct == NULL){
        error("[SERVER] Allocazione memoria fallita in \"crea_thread_struct\"");
    }

    new_struct->address = malloc(sizeof(struct sockaddr_in));
    if (new_struct->address == NULL){
        error("[SERVER] Allocazione memoria fallita(1) in \"crea_thread_struct\"");
    }
    *(new_struct->address) = *_address;
    new_struct->thread_index = _thread_index;
    new_struct->rcv_base = _rcv_base;
    new_struct->rcv_limit_window = ((_rcv_base + WINDOW) - 1)%MAX_MSG_CLUSTER;
    new_struct->send_base = _send_base;
    new_struct->send_limit_window = ((_send_base + WINDOW) - 1)%MAX_MSG_CLUSTER;
    new_struct->next_seqnum = _send_base;
    new_struct->rcv_list = crea_lista_buffer();
    new_struct->buffer_list = crea_lista_buffer();

    for(int i = 0; i < WINDOW; i++){
        (new_struct->array_ack)[i] = 0;
    }
    new_struct->timeout_adattivo = malloc(sizeof(struct timeval));
    if (new_struct->timeout_adattivo == NULL){
        error("[SERVER] Allocazione memoria fallita(2) in \"crea_thread_struct\"");
    }
    *(new_struct->timeout_adattivo) = timeout_fisso;
    new_struct->estimated_RTT = -1;
    new_struct->dev_RTT = 0;
    return new_struct;
}

void delete_thread_struct(uint32_t thread_index){
    
    printf("[SERVER] Dealloco le strutture\n");
    fflush(stdout);
        
    thread_struct *t_struct = thread_array[thread_index];
    free(t_struct->address);
    free(t_struct->rcv_list);
    free(t_struct->buffer_list);
    free(t_struct->timeout_adattivo);
    free(t_struct);

    //Libero il parcheggio
    thread_array[thread_index] = NULL;
    create_thread_node(thread_index);
    printf("[SERVER] Strutture deallocate per la connessione %d\n", thread_index);
    fflush(stdout);        
}