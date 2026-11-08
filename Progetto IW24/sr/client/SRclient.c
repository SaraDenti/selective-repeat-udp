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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sem.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "buffer_client.h"
#include "header.h"
#include "SRclient.h"
#include "timer_client.h"

#include "../shared/config.h"
#include "../shared/packet.h"

// ---------------------------------- VARIABILI CON COLLEGAMENTO ESTERNO ----------------------------------

bool opened = false;
int client_socket; //ID del socket del client
int sem_timer; //Semaforo per accesso a timer_list

//Timer
lista_timer timer_list; //variabile globale che indica la lista dei timer attivi
struct timeval timeout_fisso; 
struct timeval timeout_adattivo;

//Finestra di ricezione
nseq_t rcv_base;
nseq_t rcv_limit_window;

//Finestra di trasmissione
nseq_t send_base;
nseq_t send_limit_window;
nseq_t next_seqnum;

char array_ack[WINDOW];

lista_buffer *rcv_list; //Variabile globale che indica il buffer dei pacchetti ricevuti ma non ancora passati al livello 1
lista_buffer *buffer_list; //variabile globale che indica il buffer d'interfaccia tra livello 1 e livello 2


index_t thread_index;

bool handshake = false;//Indica se il three way handshake è completo

void send_new_packet(packet_t *packet){
    
    packet->header.index = htonl(thread_index);
    packet->header.n_seq = htonl(next_seqnum);

    while(in_window(next_seqnum, send_base, send_limit_window) == false); //Aspetta

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"send_new_packet\"");

    invia_pacchetto(packet);

#ifdef TIMER_FISSO
    timer *new_timer = create_timer(packet, &timeout_fisso);
#elif defined(TIMER_ADATTIVO)
    timer *new_timer = create_timer(packet, &timeout_adattivo);
#endif
    add_timer(new_timer);
    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"send_new_packet\"");

    next_seqnum = (next_seqnum + 1) % MAX_MSG_CLUSTER;
}

static void thread_routine(){
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_flg = 0;
    
    while(1){
        packet_t *packet = malloc(MAX_PACKET_LEN);
        if(packet == NULL){
            error("[CLIENT] Allocazione memoria fallita in \"thread_routine\"");
        }

        ricevi_pacchetto(packet);

        if(packet->header.ack_flag == 0){ //Pacchetto dati
            packet_t *ack_packet = make_packet(0, 0, 0, ntohl(packet->header.index), ntohl(packet->header.cmd), ntohl(packet->header.pos), (packet->header.end_flag), (packet->header.error_flag), 0, packet->filename, strlen(packet->filename) + 1, NULL, 0, ntohl(packet->header.n_seq), 1);
            invia_pacchetto(ack_packet);
            free(ack_packet);

            if(packet->header.fin_flag == 0){
                
                if(in_window(ntohl(packet->header.n_seq), rcv_base, rcv_limit_window)){
                    sigprocmask(SIG_BLOCK, &set, NULL);

                    if(ntohl(packet->header.n_seq) == rcv_base){
                        buff.sem_op = -1;
                        if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"thread_routine\"");

                        lista_buffer *ret_list = remove_buffer(rcv_list, packet);
                        
                        buff.sem_op = +1;
                        if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"thread_routine\"");


                        buff.sem_op = -1;
                        if (semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(2) in \"thread_routine\"");

                        if(buffer_list->first == NULL){//Lista vuota
                            buffer_list->first = ret_list->first;
                            buffer_list->last = ret_list->last;
                        }
                        else{
                            buffer_list->last->next = ret_list->first;
                            buffer_list->last = ret_list->last;
                        }
                        buff.sem_op = +1;
                        if (semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(3) in \"thread_routine\"");

                    }else{
                        buff.sem_op = -1;
                        if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(4) in \"thread_routine\"");

                        add_buffer(rcv_list, packet);

                        buff.sem_op = +1;
                        if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(5) in \"thread_routine\"");
                    }
                    sigprocmask(SIG_UNBLOCK, &set, NULL);
                }else{
                    free(packet);
                }
            }
            else{
                //E' un pacchetto di fin
                
                //Dealloca buffer_list e rcv_list
                free(buffer_list);
                free(rcv_list);

                //Termina
                pthread_exit(NULL);
            }
        }
        else{ //Pacchetto di ack
            handle_ack(ntohl(packet->header.ack));
            free(packet);
        }
    }
}

void *apri_connessione(void *arg){
    packet_t *ack_packet = (packet_t*) arg;

    // ----------------------------------------- CONNESSIONE AL SERVER -----------------------------------------
    //Creiamo una socket, identificata da un descrittore integer
    client_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if(client_socket < 0){
        error("[CLIENT] Errore durante la creazione della socket");
    }
    opened = true;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = 0; //la scelta della porta viene lasciata al sistema operativo
    addr.sin_addr.s_addr = INADDR_ANY;
    memset(addr.sin_zero,0,sizeof(addr.sin_zero));

    if(bind(client_socket,(struct sockaddr*)&addr,sizeof(addr)) < 0){
        error("[CLIENT] Errore durante il binding dell'indirizzo");
    }

    //Specifichiamo l'indirizzo del socket del server
    struct sockaddr_in server_address;
    socklen_t server_addr_len;

    server_address.sin_family = AF_INET;                // Specifico il dominio
    server_address.sin_port = htons(SERVER_PORT);       // Specifico la porta
    server_address.sin_addr.s_addr = INADDR_ANY;        // Host id

    server_addr_len = sizeof(server_address);

    //Associamo l'indirizzo del socket del server alla socket del client
    while(connect(client_socket, (struct sockaddr *)&server_address, server_addr_len) == -1){
        if(errno == EINTR){
            errno = 0;
        }else{
            error("[CLIENT] Errore durante la connessione tra socket");
        }
    }

    // ----------------------------------------- INIZIALIZZAZIONE -----------------------------------------
    //Inizializzo randomicamente send_base
    srand(time(NULL));
    send_base = rand()% MAX_MSG_CLUSTER;

    send_limit_window = (send_base + WINDOW -1) % MAX_MSG_CLUSTER;
    next_seqnum = send_base;

    //Inizializzazione timer_list
    timer_list.first = NULL;
    timer_list.last = NULL;

    timeout_fisso.tv_sec = 1;   // Secondi iniziali
    timeout_fisso.tv_usec = 0;  // Microsecondi iniziali

    sem_timer = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_timer == -1) error("[CLIENT] Errore della funzione \"semget\" in \"apri_connessione\"");
    if (semctl(sem_timer, 0, SETVAL, 1) == -1) error("[CLIENT] Errore della funzione \"semctl\" in \"apri_connessione\"");

#ifdef TIMER_FISSO
    printf("[CLIENT] Sto utilizzando il timer fisso\n");
    fflush(stdout);
#elif defined(TIMER_ADATTIVO)
    printf("[CLIENT] Sto utilizzando il timer adattivo\n");
    fflush(stdout);
    timeout_adattivo = timeout_fisso;
#endif

    //Gestione del segnale di allarme
    struct sigaction act;
    sigset_t set;
    sigfillset(&set);
    act.sa_handler = handle_alarm;
    act.sa_mask = set;
    act.sa_flags = 0;
    sigaction(SIGALRM, &act, NULL);

    //Blocca gli altri segnali
    sigdelset(&set, SIGALRM);
    sigprocmask(SIG_BLOCK, &set, NULL);

    // --------------------------------------- PRIMO PASSO THREE-WAY-HANDSHAKE ---------------------------------------
    //Invia pacchetto vuoto con syn_flag = 1 e n_seq = send_base
    packet_t *packet = make_packet(1, 0, 0, 0, 0, 0, 0, 0, send_base, NULL, 0, NULL, 0, 0, 0);
    invia_pacchetto(packet);

#ifdef TIMER_FISSO
    timer *new_timer = create_timer(packet, &timeout_fisso);
#elif defined (TIMER_ADATTIVO)
    timer *new_timer = create_timer(packet, &timeout_adattivo);
#endif

    //Imposta il timer relativo al pacchetto inviato
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigdelset(&set, SIGINT); //DA ELIMINARE
    sigprocmask(SIG_BLOCK, &set, NULL);
    add_timer(new_timer);
    sigprocmask(SIG_UNBLOCK, &set, NULL);

    next_seqnum = (next_seqnum + 1) % MAX_MSG_CLUSTER;
    
    // --------------------------------------- SECONDO PASSO THREE-WAY-HANDSHAKE ---------------------------------------

    //Aspetta di ricevere un messaggio dal server
    packet = malloc(MAX_PACKET_LEN);
    if(packet == NULL){ 
        error("[CLIENT] Allocazione memoria fallita in \"apri_connessione\"");
    }
    ricevi_pacchetto(packet);
    
    //Blocca il segnale SIGALRM
    sigprocmask(SIG_BLOCK, &set, NULL);
    struct itimerval pre_timer;

    //Disattiva il timer precedentemente impostato
    pre_timer.it_value.tv_sec = 0;
    pre_timer.it_value.tv_usec = 0;
    pre_timer.it_interval.tv_sec = 0;
    pre_timer.it_interval.tv_usec = 0;

    if(setitimer(ITIMER_REAL, &pre_timer, NULL) == -1) {
        error("[CLIENT] Errore nella disattivazione del timer in \"apri_connessione\"");
    }

    //Rimuove il timer da timer_list
    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"apri_connessione\"");
    new_timer = remove_timer(ntohl(packet->header.ack));
    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"apri_connessione\"");

    if(packet->header.rst_flag == 1){
        close(client_socket);
        free(ack_packet);
        free(packet);
        free(new_timer);
        fprintf(stderr, "[CLIENT] Server non disponibile a stabilire una nuova connessione\n");
        fflush(stderr);
        exit(EXIT_FAILURE);
    }

#ifdef TIMER_ADATTIVO
    if(new_timer->count == 0){
        //Il pacchetto non è stato ritrasmesso: il timer adattativo viene ricalcolato
        struct timeval ora;
        if(gettimeofday(&ora, NULL) == -1){
            error("[CLIENT] Errore della funzione \"gettimeofday()\" in \"apri_connessione\"");
        }

        long long int sample_RTT = tempo(&ora) - tempo(new_timer->start_time);
        timer_adattivo(sample_RTT);
    }
#endif
    free(new_timer->packet);
    free(new_timer->start_time);
    free(new_timer->curr_timer);
    free(new_timer);

    sigprocmask(SIG_UNBLOCK, &set, NULL);
        
    //Aumenta send_base e send_limit_window
    send_base = (send_base + 1) % MAX_MSG_CLUSTER;
    send_limit_window = (send_limit_window + 1)%MAX_MSG_CLUSTER;
    
    printf("[CLIENT] Connessione stabilita\n");
    fflush(stdout);

    // --------------------------------------- TERZO PASSO THREE-WAY-HANDSHAKE ---------------------------------------
    //Imposta thread_index
    thread_index = ntohl(packet->header.index);
    //Imposta finestra di ricezione
    rcv_base = (ntohl(packet->header.n_seq) + 1) % MAX_MSG_CLUSTER;
    rcv_limit_window = (rcv_base + WINDOW - 1) % MAX_MSG_CLUSTER;

    //Invia pacchetto passato dal main thread (ACK)
    ack_packet->header.syn_flag = 0;
    ack_packet->header.index = packet->header.index;
    ack_packet->header.n_seq = ntohl(next_seqnum);
    ack_packet->header.ack = packet->header.n_seq;
    ack_packet->header.ack_flag = 1;

    sigprocmask(SIG_BLOCK, &set, NULL);
    buff.sem_op = -1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(2) in \"apri_connessione\"");
    invia_pacchetto(ack_packet);

    //Imposta il timer relativo al pacchetto
#ifdef TIMER_FISSO
    new_timer = create_timer(ack_packet, &timeout_fisso);
#elif defined (TIMER_ADATTIVO)
    new_timer = create_timer(ack_packet, &timeout_adattivo);
#endif

    add_timer(new_timer);
    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(3) in \"apri_connessione\"");
    sigprocmask(SIG_UNBLOCK, &set, NULL);

    next_seqnum = (next_seqnum + 1) % MAX_MSG_CLUSTER;

    //Inizia rcv_list, una lista che si occupa della ricezione dei pacchetti non in ordine inviati dal server
    //e buffer_list, una lista che si occupa dell'interfaccia tra livello 1 e livello 2
    rcv_list = crea_lista_buffer();
    buffer_list = crea_lista_buffer();

    //Inizializzazione sem_rcv_list
    sem_rcv_list = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_rcv_list == -1) error("[CLIENT] Errore della funzione \"semget\" in \"apri_connessione\"");
    if (semctl(sem_rcv_list, 0, SETVAL, 1) == -1) error("[CLIENT] Errore della funzione \"semctl\" in \"apri_connessione\"");

    //Inizializzazione sem_buffer_list
    sem_buffer_list = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | 0666);
    if (sem_buffer_list == -1) error("[CLIENT] Errore della funzione \"semget\"(1) in \"apri_connessione\"");
    if (semctl(sem_buffer_list, 0, SETVAL, 1) == -1) error("[CLIENT] Errore della funzione \"semctl\"(1) in \"apri_connessione\"");


    handshake = true;

    thread_routine();

    return NULL;
}