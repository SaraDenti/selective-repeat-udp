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
#include <sys/time.h>
#include <sys/sem.h>
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

#ifdef TIMER_ADATTIVO
static double estimated_RTT = -1; //Stima dell RTT iniziale
static double dev_RTT = 0; //Variazione dell RTT
#endif

/**********************************************************************************************************
                                                 GESTIONE TIMER 
**********************************************************************************************************/

// -------------------------------------------- GESTIONE TEMPO --------------------------------------------

long long int tempo(struct timeval *time){
    return time->tv_sec * ((long long int) pow(10,6)) + time->tv_usec;
}

//Questa funzione determina il tempo rimanente del timer da impostare relativo al nodo node e lo salva in new_timer
static void detect_timer(timer *node, struct itimerval *new_timer){
    if(memset(new_timer, 0, sizeof(struct itimerval)) == NULL){ // Inizializza la struttura
        error("[CLIENT] Errore della funzione \"memset()\" in \"detect_timer\"");
    }  

    struct timeval ora;
    if(gettimeofday(&ora, NULL) == -1){
        error("[CLIENT] Errore della funzione \"gettimeofday()\" in \"detect_timer\"");
    }

    long long int timer_da_impostare = tempo(node->curr_timer);
    long long int ora_attuale = tempo(&ora);
    long long int quando_impostato = tempo(node->start_time);
    long long int elapsed_time = ora_attuale - quando_impostato;

    long  long int remaining_time = timer_da_impostare - elapsed_time;

    new_timer->it_value.tv_sec = remaining_time / ((long long int)pow(10,6));
    new_timer->it_value.tv_usec = remaining_time % ((long long int)pow(10,6));
    //Il timer non si ripete
    new_timer->it_interval.tv_sec = 0;     
    new_timer->it_interval.tv_usec = 0;   
}

// --------------------------------------- CALCOLO TIMER_ADATTIVO -----------------------------------------

#ifdef TIMER_ADATTIVO
void timer_adattivo(long sample_RTT){

    if(estimated_RTT < 0){
        estimated_RTT = sample_RTT;
        dev_RTT = sample_RTT / 2;
    }else{
        dev_RTT = (1 - BETA) * dev_RTT + BETA * fabs(sample_RTT - estimated_RTT);
        estimated_RTT = (1 - ALPHA) * estimated_RTT + ALPHA * sample_RTT;
    }

    long long int timeout_interval = estimated_RTT + 4*dev_RTT;

    //Si sceglie di far ritornare un valore minimo se la variabile timeout_interval scende sotto una certa soglia
    long long int temp = (timeout_interval > TIMER_MIN) ? timeout_interval : TIMER_MIN;

    timeout_adattivo.tv_sec = temp / ((long long int) pow(10,6));
    timeout_adattivo.tv_usec = temp % ((long long int) pow(10,6));  
}
#endif

// -------------------------------------------- AGGIUNGI TIMER --------------------------------------------

//Questa funzione crea un nodo di tipo timer
timer *create_timer(packet_t *packet, struct timeval *timer_da_impostare){
    
    struct timer *new_timer = malloc(sizeof(timer));
    if(new_timer == NULL){
        free(packet);
        error("[CLIENT] Allocazione memoria fallita in \"create_timer\"");
    }

    new_timer->packet = packet;
    new_timer->start_time = malloc(sizeof(struct timeval));
    if(new_timer->start_time == NULL){
        free(new_timer->packet);
        free(new_timer);
        error("[CLIENT] Allocazione memoria fallita(1) in \"create_timer\"");
    }

    if(gettimeofday(new_timer->start_time, NULL) == -1){ //Questa funzione ottiene il tempo attuale. Il secondo parametro è impostato a NULL per default
        error("[CLIENT] Errore della funzione \"gettimeofday()\" in \"create_timer\"");
    }
    new_timer->curr_timer = malloc(sizeof(struct timeval));
    if(new_timer->curr_timer == NULL){
        free(new_timer->packet);
        free(new_timer->start_time);
        free(new_timer);
        error("[CLIENT] Allocazione memoria fallita(2) in \"create_timer\"");
    }
    *new_timer->curr_timer = *timer_da_impostare;
    new_timer->next = NULL;
    new_timer->count = 0;
    return new_timer;
}

// -------------------------------------------- GESTIONE LISTA --------------------------------------------

#ifdef TIMER_ADATTIVO
static void insert_in_order(struct timer *new_timer){

    // Trova la posizione corretta per l'inserimento 
    struct timer *curr = timer_list.first;
    struct timer *prev = NULL;

    struct itimerval temp1, temp2;

    while(curr != NULL){
        detect_timer(curr, &temp1);
        detect_timer(new_timer, &temp2);
    
        long long int time_1 = tempo(&(temp1.it_value));
        long long int time_2 = tempo(&(temp2.it_value));

        if(time_1 > time_2){
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    // Inserisce il nuovo timer nella posizione trovata
    new_timer->next = curr;
    if(prev == NULL){
        // Se prev è NULL, il nuovo timer diventa il primo elemento
        //Disattiva il timer precedentemente impostato
            struct itimerval pre_timer;

            pre_timer.it_value.tv_sec = 0;
            pre_timer.it_value.tv_usec = 0;
            pre_timer.it_interval.tv_sec = 0;
            pre_timer.it_interval.tv_usec = 0;

            if(setitimer(ITIMER_REAL, &pre_timer, NULL) == -1) {
                error("[CLIENT] Errore nella disattivazione del timer in \"insert_in_order\"");
            } 

            timer_list.first = new_timer;

            if(setitimer(ITIMER_REAL, &temp2, NULL) == -1){
                    error("[CLIENT] Errore nell'impostazione del timer in \"insert_in_order\"");
            }
    }else{
        prev->next = new_timer;
    }
    // Se il nuovo timer è stato aggiunto alla fine, aggiorna last
    if(curr == NULL){
        timer_list.last = new_timer;
    }
}
#endif

void add_timer(struct timer *new_timer){

    new_timer->next = NULL;
    if(timer_list.first == NULL){ //Se la lista è vuota il timer viene avviato
        timer_list.first = new_timer;
        
        printf("[CLIENT] Avvio il timer per il pacchetto %d\n", ntohl(new_timer->packet->header.n_seq));
        fflush(stdout);

        //Imposto il timer 'time'
        struct itimerval time;
        time.it_value = *(new_timer->curr_timer);
        //Il timer non si ripete
        time.it_interval.tv_sec = 0;     
        time.it_interval.tv_usec = 0;  

        if(setitimer(ITIMER_REAL, &time, NULL) == -1){
            error("[CLIENT] Errore nell'impostazione del timer in \"add_timer\"");
        }
        timer_list.last = new_timer; //Il nuovo timer creato è l'ultimo elemento della lista
    }   
    else{ //Se la lista non è vuota il nuovo timer viene inserito nella posizione opportuna
#ifdef TIMER_FISSO
        timer_list.last->next = new_timer;
        timer_list.last = new_timer;
#elif defined(TIMER_ADATTIVO)
        insert_in_order(new_timer);
#endif
    }  
}

// -------------------------------------------- ELIMINA TIMER --------------------------------------------

//Questa funzione si limita esclusivamente a rimuovere da timer_list il timer associato a _nseq e ritornarlo 
//alla funzione chiamante.
//E' lasciato alla funzione chiamante il compito di disattivare il timer invocando 'setitimer()'
timer *remove_timer(nseq_t _nseq){

    if(timer_list.first != NULL){//La lista non è vuota
        timer *old = NULL;
        timer *curr = timer_list.first;
        
        while(curr != NULL){
            if(ntohl(curr->packet->header.n_seq) == _nseq){//Timer da eliminare

                if(old == NULL){ //Il timer da eliminare è il primo elemento della lista
                    timer_list.first = curr->next;
                }
                else{
                    old->next = curr->next;
                }

                if(curr == timer_list.last){//Il timer da eliminare è l'ultimo elemento della lista
                    timer_list.last = old;
                }
                return curr;
            }
            else{
                old = curr;
                curr = curr ->next;
            }        
        }
    }
    return NULL;  
}

static void elimina_connessione(void){

    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigprocmask(SIG_BLOCK, &set, NULL);

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"elimina_connessione\"");

    if(rcv_list != NULL){
        //Dealloca rcv_list
        buffer *curr = rcv_list->first;
        buffer *old = NULL;
        while(curr != NULL){
            old = curr;
            curr = curr->next;
            free(old->packet);
            free(old);
        }
        free(rcv_list);
    }

    buff.sem_op = +1;
    if (semop(sem_rcv_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"elimina_connessione\"");

    
    buff.sem_op = -1;
    if (semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(2) in \"elimina_connessione\"");

    if(buffer_list != NULL){
        //Dealloca buffer_list
        buffer *curr = buffer_list->first;
        buffer *old = NULL;
        while(curr != NULL){
            old = curr;
            curr = curr->next;
            free(old->packet);
            free(old);
        }
        free(buffer_list);
    }

    buff.sem_op = +1;
    if (semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(3) in \"elimina_connessione\"");


    //Dealloca timer_list
    timer *curr = timer_list.first;
    timer *old = NULL;
    while(curr != NULL){
        old = curr;
        curr = curr->next;
        free(old->packet);
        free(old->start_time);
        free(old->curr_timer);
        free(old);
    }

    sigprocmask(SIG_UNBLOCK, &set, NULL);

    fprintf(stderr, "[CLIENT] Il server se nè andato e non ritorna più...\n");
    fflush(stderr);
    exit(EXIT_FAILURE);
}

static void imposta_timer(void){
    bool set = false;
    timer *new_timer = NULL;
    struct itimerval detect;
    
    while(set == false){
        //Verifica se il nuovo timer è scaduto e, in caso negativo, lo imposta
        detect_timer(timer_list.first, &detect);
        
        if((detect.it_value.tv_sec < 0) || (detect.it_value.tv_usec < 0) || ((detect.it_value.tv_sec == 0) && (detect.it_value.tv_usec == 0))){//Il nuovo timer è scaduto
            //Inserisce il timer scaduto alla fine della lista
            new_timer = remove_timer(ntohl(timer_list.first->packet->header.n_seq));
            if(new_timer != NULL){
                new_timer->count++;
                if(new_timer->count > MAX_COUNT){
                    elimina_connessione();
                }else{
                    invia_pacchetto(new_timer->packet);
                    if(gettimeofday(new_timer->start_time, NULL) == -1){
                        error("[CLIENT] Errore della funzione \"gettimeofday()\" in \"imposta_timer\"");
                    }
#ifdef TIMER_ADATTIVO
                    *(new_timer->curr_timer) = timeout_adattivo;
#endif
                    add_timer(new_timer);
                }
            }
        }
        else{
            printf("[CLIENT] Imposto il timer relativo al numero di sequenza %d\n",ntohl(timer_list.first->packet->header.n_seq));
            fflush(stdout);
            if(setitimer(ITIMER_REAL, &detect, NULL) == -1){
                error("[CLIENT] Errore nell'impostazione del timer in \"imposta_timer\"");
            }
            set = true;
        }
    }
}

/**********************************************************************************************************
                                                 GESTIONE SIGALRM
**********************************************************************************************************/

//Questa funzione viene invocata quando il processo/thread viene colpito dal segnale SIGALRM
void handle_alarm(int sig){
    
    printf("[CLIENT] Inizio gestione handle_alarm()\n");
    fflush(stdout);

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"handle_alarm\"");
    
    imposta_timer();

    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"handle_alarm\"");

    printf("[CLIENT] Fine gestione handle_alarm()\n");
    fflush(stdout);
}

/**********************************************************************************************************
                                                 GESTIONE ACK
**********************************************************************************************************/

//Questa funzione viene invocata quando arriva un segnale di ack
void handle_ack(nseq_t ack){
     
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigprocmask(SIG_BLOCK, &set, NULL);

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_op = -1;
    buff.sem_flg = 0;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"handle_ack\"");
    
    bool first = false;

    if(in_window(ack, send_base, send_limit_window)){

        printf("[CLIENT] Inizio gestione handle_ack()\n");
        fflush(stdout);

        //Se il primo elemento di timer_list è il timer da eliminare
        if (timer_list.first != NULL){
            if ((ntohl(timer_list.first->packet->header.n_seq) == ack)){

                first = true;

                //Disattiva il timer precedentemente impostato
                struct itimerval pre_timer;

                pre_timer.it_value.tv_sec = 0;
                pre_timer.it_value.tv_usec = 0;
                pre_timer.it_interval.tv_sec = 0;
                pre_timer.it_interval.tv_usec = 0;

                if(setitimer(ITIMER_REAL, &pre_timer, NULL) == -1) {
                    error("[SERVER] Errore nella disattivazione del timer in \"handle_ack\"");
                } 
            }
        }

        //Elimina il timer relativo all'ack e rilascia le risorse ad esso associate per evitare memory leak
        timer *ack_timer = remove_timer(ack);
        if(ack_timer != NULL){
#ifdef TIMER_ADATTIVO
            if(ack_timer->count == 0){
                //Il pacchetto non è stato riinviato
                //Calcolo il valore del sample_RTT
                struct timeval ora;
                if(gettimeofday(&ora, NULL) == -1){
                    error("[SERVER] Errore della funzione \"gettimeofday()\" in \"handle_ack\"");
                }

                long long int sample_RTT = tempo(&ora) - tempo(ack_timer->start_time);
                timer_adattivo(sample_RTT);
            } 
#endif
            free(ack_timer->packet);
            free(ack_timer->start_time);
            free(ack_timer->curr_timer);
            free(ack_timer);
        }

        //Se il timer da eliminare era il primo elemento di timer_list ma non l'unico imposta un nuovo timer
        if((first == true) && (timer_list.first != NULL)){ 
            imposta_timer();
        }
        
        //Aggiorna array_ack
        if(ack != send_base){
            int pos;
            if(send_base < ack){
                pos = ack - send_base;
            }
            else{
                pos = (MAX_MSG_CLUSTER - send_base) + ack;
            }
            array_ack[pos] = 1;
        }
        else{
            //Determino la posizione del primo elemento diverso da 1
            int pos = 1;
            while(pos < WINDOW){
                if(array_ack[pos] == 1){
                    pos++;
                }
                else{
                    break;
                }
            }
            send_base = (send_base + pos) % MAX_MSG_CLUSTER;
            send_limit_window = (send_limit_window + pos) % MAX_MSG_CLUSTER;
            //Aggiorno array_ack
            for(int i = 0; i < (WINDOW - pos); i++){
                array_ack[i] = array_ack[pos+i];
            }
            for(int i = (WINDOW - pos); i < WINDOW; i++){
                array_ack[i] = 0;
            }
        }
        printf("[CLIENT] Fine gestione handle_ack()\n");
        fflush(stdout);
    }


    buff.sem_op = +1;
    if (semop(sem_timer, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"handle_ack\"");

    sigprocmask(SIG_UNBLOCK, &set, NULL);
}