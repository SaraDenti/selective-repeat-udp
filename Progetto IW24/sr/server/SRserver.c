#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <dirent.h>
#include <stdbool.h>
#include <time.h>
#include <signal.h>
#include <math.h>
#include <errno.h>

#include <sys/sem.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "buffer_server.h"
#include "header.h"
#include "server.h"
#include "timer_server.h"

#include "../shared/config.h"
#include "../shared/packet.h"

// ---------------------------------- VARIABILI CON COLLEGAMENTO ESTERNO ----------------------------------

bool opened = false; 
int server_socket; //ID del socket del server

//Timer
lista_timer timer_list; 
struct timeval timeout_fisso; 
struct timeval timeout_adattivo;

thread_struct *thread_array[WORKER_THREADS_NUM];
struct _thread_list thread_list;

int sem_timer;
int sem_thread_list;
int sem_buffer_list;
int sem_rcv_list;

struct sockaddr_in client_address; //struttura in cui si memorizza l'indirizzo del client da cui si riceve un pacchetto
socklen_t client_addr_len = sizeof(client_address);

__thread thread_struct *t_struct;

pthread_t main_tid;

void send_new_packet(packet_t *packet){
    packet->header.index = htonl(t_struct->thread_index);
    packet->header.n_seq = htonl(t_struct->next_seqnum);
    packet->header.address = *(t_struct->address);

    while (in_window(t_struct->next_seqnum, t_struct->send_base, t_struct->send_limit_window) == false); //Aspetta

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"send_new_packet\"");

    invia_pacchetto(packet);

#ifdef TIMER_FISSO
    timer *new_timer = create_timer(packet, &timeout_fisso);
#elif defined(TIMER_ADATTIVO)
    timer *new_timer = create_timer(packet, t_struct->timeout_adattivo);
#endif
    add_timer(new_timer);
    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"send_new_packet\"");


    t_struct->next_seqnum = (t_struct->next_seqnum + 1) % MAX_MSG_CLUSTER;
}

void *thread_function(void *arg){
    t_struct = (thread_struct*)arg;

    //Blocca i segnali
    sigset_t set;
    sigfillset(&set);
    sigdelset(&set, SIGTERM);
    sigprocmask(SIG_BLOCK, &set, NULL);

    while(1){
        while(t_struct->buffer_list->first == NULL); //Aspetta

        struct sembuf buff;
        buff.sem_num = 0;
        buff.sem_op = -1;
        buff.sem_flg = 0;
        if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"thread_function\"");

        //Estrae il primo pacchetto da buffer_list
        struct buffer *temp_buff = t_struct->buffer_list->first;
        
        //Rimuovo il primo pacchetto da buffer_list
        temp_buff = t_struct->buffer_list->first;

        if(t_struct->buffer_list->first == t_struct->buffer_list->last){
            t_struct->buffer_list->first = NULL;
            t_struct->buffer_list->last = NULL;
        }else{
            t_struct->buffer_list->first = t_struct->buffer_list->first->next;
        }
        packet_t *packet = temp_buff->packet;
        free(temp_buff);
        
        buff.sem_op = +1;
        if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"thread_function\"");

        if (packet->header.fin_flag == 1){
            //Elimina da timer_list tutti i pacchetti di cui non ha ricevuto riscontro
            struct sembuf buff;
            buff.sem_num = 0;
            buff.sem_op = -1;
            buff.sem_flg = 0;
            if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(2) in \"thread_function\"");
            elimina_connessione(t_struct);
            buff.sem_op = +1;
            if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(3) in \"thread_function\"");
            
            //Invia il pacchetto di fin
            packet_t *fin_packet = make_packet(0, 0, 1, 0, 0, 0, 1, 0, 0, NULL, 0, NULL, 0, 0, 0);
            send_new_packet(fin_packet);
            printf("[THREAD %ld] Thread terminato\n", pthread_self());
            fflush(stdout);
            pthread_exit(NULL);
        }

        switch(ntohs(packet->header.cmd)){
            case CMD_LIST:
                bool ret;
                ret = crea_lista();
                if (ret == false){//Non sono presenti file da condividere
                    char *mess = "Non è presente alcun file disponibile per la condivisione\n";
                    packet_t *error_packet = make_packet(0, 0, 0, 0, CMD_LIST, 0, 1, 1, 0, packet->filename, strlen(packet->filename) + 1, mess, strlen(mess) + 1, 0, 0);
                    send_new_packet(error_packet);
                }
                else{//filename.txt creato correttamente
                    spedisci_file(packet);
                }
                break;
            case CMD_GET:
                if (file_esistente(packet->filename) == true){
                    spedisci_file(packet);
                }
                else{
                    int len = snprintf(NULL, 0, "Il file %s non è disponibile per la condivisione\n", packet->filename);
                    if (len < 0) error("[SERVER] Errore della funzione \"snprintf()\" in \"thread_function\"");
                    char mess[len + 1];
                    if (sprintf(mess, "Il file %s non è disponibile per la condivisione\n", packet->filename) < 0) error("[SERVER] Errore della funzione \"snprintf()\"(1) in \"thread_function\"");
                    mess[len] = '\0';
                    packet_t *error_packet = make_packet(0, 0, 0, 0, CMD_GET, 0, 1, 1, 0, packet->filename, strlen(packet->filename) + 1, mess, len + 1, 0, 0);
                    send_new_packet(error_packet);
                }
                break;
            case CMD_PUT:
                ricevi_file(packet);
                break;
        }
    }
    pthread_exit(NULL);
}

void server_rcv(packet_t *packet){
    //Invia ack
    packet_t *ack_packet = make_packet(0, 0, 0, ntohl(packet->header.index), ntohl(packet->header.cmd), ntohl(packet->header.pos), (packet->header.end_flag), (packet->header.error_flag), 0, packet->filename, strlen(packet->filename) + 1, NULL, 0, ntohl(packet->header.n_seq), 1);
    ack_packet->header.address = packet->header.address;
    invia_pacchetto(ack_packet);
    free(ack_packet);

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigprocmask(SIG_BLOCK, &set, NULL);
    
    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_flg = 0;

    thread_struct *t_struct = thread_array[ntohl(packet->header.index)];

    if(in_window(ntohl(packet->header.n_seq), t_struct->rcv_base, t_struct->rcv_limit_window)){
        if(ntohl(packet->header.n_seq) == t_struct->rcv_base){
            buff.sem_op = -1;
            if(semop(sem_rcv_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"server_rcv\"");
            
            lista_buffer *ret_list = remove_buffer(t_struct->rcv_list, packet, t_struct);
            
            buff.sem_op = +1;
            if(semop(sem_rcv_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"server_rcv\"");

            buff.sem_op = -1;
            if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(2) in \"server_rcv\"");

            if (t_struct->buffer_list->first == NULL){//Lista vuota
                t_struct->buffer_list->first = ret_list->first;
            }
            else{
                t_struct->buffer_list->last->next = ret_list->first;
            }
            t_struct->buffer_list->last = ret_list->last;

            buff.sem_op = +1;
            if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(3) in \"server_rcv\"");

        }else{
            buff.sem_op = -1;
            if(semop(sem_rcv_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(4) in \"server_rcv\"");
            
            add_buffer(t_struct->rcv_list, packet, t_struct);

            buff.sem_op = +1;
            if(semop(sem_rcv_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(5) in \"server_rcv\"");

        }
    }else{
        free(packet);
    }
    sigprocmask(SIG_UNBLOCK, &set, NULL);
}

int main(void){
    // ---------------------------------------------- SOCKET ---------------------------------------------- 
    struct sockaddr_in server_address; //indirizzo del server
    
    server_address.sin_family = AF_INET;            // Dominio del socket
    server_address.sin_port = htons(SERVER_PORT);   // Porta da utilizzare
    server_address.sin_addr.s_addr = INADDR_ANY;    // Il socket ascolta su tutte le interfacce di rete disponibili
    memset(server_address.sin_zero, 0, sizeof(server_address.sin_zero));

    //Creazione della socket
    server_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if(server_socket < 0){
        error("[SERVER] Errore durante la creazione della socket");
    }
    opened = true;

    //Binding
    int opt = 1;
    if(setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0){ 
        error("[SERVER] Errore durante l'impostazione di SO_REUSEADDR");
    } 

    if(bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        error("[SERVER] Errore durante il binding dell'indirizzo");
    }

    // ----------------------------------------- INIZIALIZZAZIONE -----------------------------------------

    //Inizializzazione thread_list
    sem_thread_list = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_thread_list == -1) error("[SERVER] Errore della funzione \"semget\" in \"main\"");
    if (semctl(sem_thread_list, 0, SETVAL, 1) == -1) error("[SERVER] Errore della funzione \"semctl\" in \"main\"");

    for (int i = 0; i<WORKER_THREADS_NUM; i++){
        create_thread_node(i);
    }    

    //Inizializzazione timer_list
    timer_list.first = NULL;
    timer_list.last = NULL; 

    sem_timer = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_timer == -1) error("[SERVER] Errore della funzione \"semget\"(1) in \"main\"");
    if (semctl(sem_timer, 0, SETVAL, 1) == -1) error("[SERVER] Errore della funzione \"semctl\"(1) in \"main\"");

    //Inizializzazione timeout fisso
    timeout_fisso.tv_sec = 1;   // Secondi iniziali
    timeout_fisso.tv_usec = 0;  // Microsecondi iniziali

#ifdef TIMER_FISSO
    printf("[SERVER] Sto utilizzando il timer fisso\n"); 
    fflush(stdout);
#elif defined(TIMER_ADATTIVO)
    printf("[SERVER] Sto utilizzando il timer adattivo\n");
    fflush(stdout);
#endif

    //Inizializzazione sem_buffer_list
    sem_buffer_list = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_buffer_list == -1) error("[SERVER] Errore della funzione \"semget\"(2) in \"main\"");
    if (semctl(sem_buffer_list, 0, SETVAL, 1) == -1) error("[SERVER] Errore della funzione \"semctl\"(2) in \"main\"");

    //Inizializzazione sem_rcv_list
    sem_rcv_list = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_rcv_list == -1) error("[SERVER] Errore della funzione \"semget\"(3) in \"main\"");
    if (semctl(sem_rcv_list, 0, SETVAL, 1) == -1) error("[SERVER] Errore della funzione \"semctl\"(3) in \"main\"");

    //Gestione del segnale di allarme
    struct sigaction act;
    sigset_t set;
    sigfillset(&set);
    act.sa_handler = handle_alarm;
    act.sa_mask = set;
    act.sa_flags = 0;
    sigaction(SIGALRM, &act, NULL);

    //Si ignorano tutti gli altri segnali
    sigdelset(&set, SIGALRM);
    sigdelset(&set, SIGINT); //DA ELIMINARE
    sigprocmask(SIG_BLOCK, &set, NULL);

    //Creazione directory "file_disponibili"
    // Controlla se la directory esiste
    if (access(subdirectory, F_OK) == 0) {
        printf("[SERVER] La directory '%s' esiste\n", subdirectory);
    } else {
        // Se non esiste, prova a crearla
        if (mkdir(subdirectory, 0777) == 0) {
            printf("[SERVER] Directory '%s' creata con successo\n", subdirectory);
        } else error("[SERVER] Errore nella creazione della directory \"file_disponibili\"");
    }

    // ------------------------------------------ MULTITHREADING ------------------------------------------

    packet_t *packet; //variabile che punta al pacchetto ricevuto

    srand(time(NULL));
    
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_flg = 0;

    while(1){
        packet = malloc(MAX_PACKET_LEN);
        if(packet == NULL){ 
            error("[SERVER] Allocazione memoria fallita in \"main\"");
        }
        ricevi_pacchetto(packet);

        if(packet->header.syn_flag == 1){
            //printf("[SERVER] Syn flag == 1\n");
            //fflush(stdout);

            //Blocca il segnale SIGALRM e richiede un token a sem_timer
            sigprocmask(SIG_BLOCK, &set, NULL);
            buff.sem_op = -1;
            if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"main\""); 

            //Scorre tutti gli elementi di timer_list verificando la presenza di un pacchetto SYN duplicato
            bool duplicate_syn = false;
            struct timer *curr = timer_list.first;
            while(curr != NULL){
                if((curr->packet->header.syn_flag == 1) &&  (indirizzo_uguale(&client_address, &curr->packet->header.address)) && (packet->header.n_seq == curr->packet->header.ack)){
                    //Il pacchetto è un pacchetto SYN duplicato
                    duplicate_syn = true;
                    free(packet);
                    
                    break;
                }
                curr = curr->next;
            }

            //Sblocca il segnale SIGALRM e rilascia il token da sem_timer
            buff.sem_op = +1;
            if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"main\"");

            if (duplicate_syn == false){ //Il pacchetto è un nuovo SYN

                //Richiede un token da sem_thread_list
                buff.sem_op = -1;
                if (semop(sem_thread_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop\"(2) in \"main\"");

                //Verifica se thread_list ha almeno un elemento
                if(thread_list.first == NULL){
                    //thread_list è vuota: il server non accetta la richiesta di nuova connessione

                    //Rilascia il token di sem_thread_list
                    if (semop(sem_thread_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop\"(3) in \"main\"");
                    
                    //Sblocca i segnali
                    sigprocmask(SIG_UNBLOCK, &set, NULL);

                    //Invia al client un pacchetto che indica di non aver accettato la richiesta
                    packet_t *rst_packet = make_packet(0, 1, 0, 0, 0, 0, 0, 0, 0, NULL, 0, NULL, 0, ntohl(packet->header.n_seq), 1); 
                    rst_packet->header.address = client_address;
                    invia_pacchetto(rst_packet);
                    free(rst_packet);
                    fprintf(stderr, "[SERVER] Connessione non disponibile\n");
                    fflush(stderr);                    
                }
                else{
                    //thread_list ha almeno un elemento: il server accetta la richiesta di nuova connessione

                    //Elimina l'indice dalla lista thread_list
                    thread_node *node = thread_list.first;
                    uint32_t thread_index = node->index;
                    thread_list.first = node->next;
                    free(node);
                    
                    //Rilascia il token di sem_thread_list
                    buff.sem_op = +1;
                    if (semop(sem_thread_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop\"(4) in \"main\"");

                    //Sblocca il segnale SIGALRM
                    sigprocmask(SIG_UNBLOCK, &set, NULL);

                    //Calcola randomicamente send_base
                    nseq_t send_base = rand()% MAX_MSG_CLUSTER;

                    //Invia un paccketto SYNACK per indicare al client di aver accettato la richiesta
                    packet_t *syn_packet = make_packet(1, 0, 0, thread_index, 0, 0, 0, 0, send_base, NULL, 0, NULL, 0, ntohl(packet->header.n_seq), 1);
                    syn_packet->header.address = client_address;

                    //Blocchiamo il segnale SIGALRM
                    sigprocmask(SIG_BLOCK, &set, NULL);

                    //Richiede un token a sem_timer
                    buff.sem_op = -1;
                    if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(5) in \"main\"");

                    invia_pacchetto(syn_packet);

                    //Crea un nuovo timer (Indipendentemente dal fatto che stiamo utilizzando il timer fisso o quello adattivo il primo 
                    //timer relativo alla connessione viene posto uguale a timeout_fisso)
                    timer* timer = create_timer(syn_packet, &timeout_fisso);

                    //Aggiunge il nuovo timer a timer_list
                    add_timer(timer);
                    buff.sem_op = +1;
                    if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(6) in \"main\"");

                    //Sblocca il segnale SIGALRM
                    sigprocmask(SIG_UNBLOCK, &set, NULL);
                }
            }
        }
        else{
            //printf("[SERVER] Syn flag == 0\n");
            //fflush(stdout);

            if(thread_array[ntohl(packet->header.index)] == NULL){ //Connessione half-established o non stabilita
                if(packet->header.fin_flag == 1){

                    if(packet->header.ack_flag == 0){
                        //Invia ack
                        packet_t *fin_ack_packet = make_packet(0, 0, 0, ntohl(packet->header.index), 0, 0, 0, 0, 0, NULL, 0, NULL, 0, ntohl(packet->header.n_seq), 1);
                        fin_ack_packet->header.address = client_address;
                        invia_pacchetto(fin_ack_packet);
                        free(fin_ack_packet);
                    }

                    free(packet);
                }
                else{
                    //Blocco i segnali e richiedo token a sem_timer
                    sigprocmask(SIG_BLOCK, &set, NULL);
                    buff.sem_op = -1;
                    if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(7) in \"main\"");

                    //Verifica se all'interno di timer_list è presente un pacchetto relativo alla connessione half-established
                    struct timer *curr = timer_list.first;
                    bool present = false;
                    while(curr != NULL){
                        if(indirizzo_uguale(&curr->packet->header.address, &client_address) && (ntohl(packet->header.index) == ntohl(curr->packet->header.index)) && (ntohl(packet->header.ack) == ntohl(curr->packet->header.n_seq))){
                            //Sblocco i segnali e rilascio il token a sem_timer
                            sigprocmask(SIG_UNBLOCK, &set, NULL);
                            buff.sem_op = +1;
                            if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(8) in \"main\"");

                            //Terzo passo three way handshake
                            present = true;
                            
                            //Invia un ack al client
                            packet_t *ack_packet = make_packet(0, 0, 0, ntohl(packet->header.index), ntohs(packet->header.cmd), ntohl(packet->header.pos), ntohl(packet->header.end_flag), 0, 0, packet->filename, strlen(packet->filename) + 1, NULL, 0, ntohl(packet->header.n_seq), 1);
                            ack_packet->header.address = client_address;
                            invia_pacchetto(ack_packet);

                            //Crea una struttura thread_struct relativa alla nuova connessione inizializzando tutto tranne tid
                            thread_struct *t_struct = crea_thread_struct(&client_address, ntohl(packet->header.index), ntohl(packet->header.n_seq), ntohl(packet->header.ack) );
                            
                            thread_array[ntohl(packet->header.index)] = t_struct;

                            pthread_t tid;
                            if(pthread_create(&tid, NULL, &thread_function, (void*)t_struct) != 0) error("[SERVER] Errore nella creazione del thread");
                            pthread_detach(tid);

                            t_struct->tid = tid;

                            handle_ack(ntohl(packet->header.ack), t_struct, true);
                            t_struct->next_seqnum = (t_struct->next_seqnum + 1) % MAX_MSG_CLUSTER;

                            packet->header.address = client_address;

                            sigprocmask(SIG_BLOCK, &set, NULL);
                            buff.sem_op = -1;
                            if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(9) in \"main\"");
                            add_buffer(t_struct->buffer_list, packet, t_struct);
                            buff.sem_op = +1;
                            if(semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(10) in \"main\"");
                            sigprocmask(SIG_UNBLOCK, &set, NULL);

                            t_struct->rcv_base = (t_struct->rcv_base + 1) % MAX_MSG_CLUSTER;
                            t_struct->rcv_limit_window = (t_struct->rcv_limit_window + 1) % MAX_MSG_CLUSTER;
                            break;
                        }
                        curr = curr->next;
                    }
                    if(present == false){
                        free(packet);

                        //Rilascia il token e sblocca i segnali
                        sigprocmask(SIG_UNBLOCK, &set, NULL);
                        buff.sem_op = +1;
                        if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(11) in \"main\"");
                    }
                }
            }
            else{
                if(indirizzo_uguale(&client_address, (thread_array[ntohl(packet->header.index)])->address)){

                    //Connessione stabilita
                    if(packet->header.ack_flag == 1){
                        
                        handle_ack(ntohl(packet->header.ack), thread_array[ntohl(packet->header.index)], true);
                        
                        free(packet);
                        
                    }else{
                        packet->header.address = client_address;
                        server_rcv(packet);
                    }
                }else{
                    //L'elemento di thread_array non si riferisce alla connessione relativa al pacchetto arrivato
                    if((packet->header.fin_flag == 1) && (packet->header.ack_flag == 0)){

                        packet_t *ack_packet = make_packet(0, 0, 0, ntohl(packet->header.index), ntohs(packet->header.cmd), ntohl(packet->header.pos), ntohl(packet->header.end_flag), 0, 0, packet->filename, strlen(packet->filename) + 1, NULL, 0, ntohl(packet->header.n_seq), 1);
                        ack_packet->header.address = client_address;
                        invia_pacchetto(ack_packet);
                        free(ack_packet);
                        free(packet);
                    }
                }
            }
        }
    }
}