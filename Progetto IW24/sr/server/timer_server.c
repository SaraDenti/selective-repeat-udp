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
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "header.h"
#include "timer_server.h"

#include "../shared/config.h"
#include "../shared/packet.h"

/**********************************************************************************************************
                                                 GESTIONE TIMER
**********************************************************************************************************/

// -------------------------------------------- GESTIONE TEMPO --------------------------------------------

static long long int tempo(struct timeval *time){
    return time->tv_sec * ((long long int) pow(10,6)) + time->tv_usec;
}

//Questa funzione determina il tempo rimanente del timer da impostare relativo al nodo node e lo salva in new_timer
static void detect_timer(timer *node, struct itimerval *new_timer){
    if(memset(new_timer, 0, sizeof(struct itimerval)) == NULL){ // Inizializza la struttura
        error("[SERVER] Errore della funzione \"memset()\" in \"detect_timer\"");
    }  

    struct timeval ora;
    if(gettimeofday(&ora, NULL) == -1){
        error("[SERVER] Errore della funzione \"gettimeofday()\" in \"detect_timer\"");
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
static void timer_adattivo(long sample_RTT, thread_struct *t_struct){

    if(t_struct->estimated_RTT < 0){
        t_struct->estimated_RTT = sample_RTT;
        t_struct->dev_RTT = sample_RTT / 2;
    }else{
        t_struct->dev_RTT = (1 - BETA) * t_struct->dev_RTT + BETA * fabs(sample_RTT - t_struct->estimated_RTT);
        t_struct->estimated_RTT = (1 - ALPHA) * t_struct->estimated_RTT + ALPHA * sample_RTT;
    }

    long long int timeout_interval = t_struct->estimated_RTT + 4*t_struct->dev_RTT;

    //Si sceglie di far ritornare un valore minimo se la variabile timeout_interval scende sotto una certa soglia
    long long int temp = (timeout_interval > TIMER_MIN) ? timeout_interval : TIMER_MIN;

    t_struct->timeout_adattivo->tv_sec = temp / ((long long int) pow(10,6));
    t_struct->timeout_adattivo->tv_usec = temp % ((long long int) pow(10,6));  
}
#endif

// -------------------------------------------- AGGIUNGI TIMER --------------------------------------------

//Questa funzione crea un nodo di tipo timer
timer *create_timer(packet_t *packet, struct timeval *timer_da_impostare){
    
    struct timer *new_timer = malloc(sizeof(timer));
    if(new_timer == NULL){
        free(packet);
        error("[SERVER] Allocazione memoria fallita in \"create_timer\"");
    }

    new_timer->packet = packet;
    new_timer->start_time = malloc(sizeof(struct timeval));
    if(new_timer->start_time == NULL){
        free(new_timer->packet);
        free(new_timer);
        error("[SERVER] Allocazione memoria fallita(1) in \"create_timer\"");
    }
    if(gettimeofday(new_timer->start_time, NULL) == -1){ //Questa funzione ottiene il tempo attuale. Il secondo parametro è impostato a NULL per default
        error("[SERVER] Errore della funzione \"gettimeofday()\" in \"create_timer\"");
    } 
    new_timer->curr_timer = malloc(sizeof(struct timeval));
    if(new_timer->curr_timer == NULL){
        free(new_timer->packet);
        free(new_timer->start_time);
        free(new_timer);
        error("[SERVER] Allocazione memoria fallita(2) in \"create_timer\"");
    }
    *new_timer->curr_timer = *timer_da_impostare;
    new_timer->next = NULL;
    new_timer->count =0;
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
                error("[SERVER] Errore nella disattivazione del timer in \"insert_in_order\"");
            } 

            timer_list.first = new_timer;

            if(setitimer(ITIMER_REAL, &temp2, NULL) == -1){
                    error("[SERVER] Errore nell'impostazione del timer in \"insert_in_order\"");
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
        
        printf("[SERVER] Avvio il timer per il pacchetto %d\n", ntohl(new_timer->packet->header.n_seq));
        fflush(stdout);

        //Imposto il timer 'time'
        struct itimerval time;
        time.it_value = *(new_timer->curr_timer);
        //Il timer non si ripete
        time.it_interval.tv_sec = 0;     
        time.it_interval.tv_usec = 0;    

        if(setitimer(ITIMER_REAL, &time, NULL) == -1){
            error("[SERVER] Errore nell'impostazione del timer in \"add_timer\"");
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
//E' lasciato ad essa il compito di disattivare il timer invocando 'setitimer()'
timer *remove_timer(struct sockaddr_in *address, index_t index, nseq_t _nseq){

    if(timer_list.first != NULL){//La lista non è vuota
        timer *old = NULL;
        timer *curr = timer_list.first;
        
        while(curr != NULL){
            if((indirizzo_uguale(&curr->packet->header.address, address)) && (ntohl(curr->packet->header.index) == index) && (ntohl(curr->packet->header.n_seq) == _nseq)){//Timer da eliminare

                if(old == NULL){//Il timer da eliminare è il primo elemento della lista
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

static void imposta_timer(void){
    timer *new_timer = NULL;
    struct itimerval detect;
    
    while(1){
        //Verifica se il nuovo timer è scaduto e, in caso negativo, lo imposta
        if (timer_list.first != NULL){
            detect_timer(timer_list.first, &detect);
        }
        else{
            break;
        }
        if((detect.it_value.tv_sec < 0) || (detect.it_value.tv_usec < 0) || ((detect.it_value.tv_sec == 0) && (detect.it_value.tv_usec == 0))){//Il nuovo timer è scaduto
            //Rimuove il primo elemento di timer_list dalla lista
            new_timer = remove_timer(&timer_list.first->packet->header.address, ntohl(timer_list.first->packet->header.index), ntohl(timer_list.first->packet->header.n_seq));
            if(new_timer != NULL){
                new_timer->count++;
                thread_struct *t_struct = thread_array[ntohl(new_timer->packet->header.index)];

                if(new_timer->count > MAX_COUNT){
                    //Se è stato raggiunto il numero di ritrasmissioni massimo elimina la connessione
                    printf("[SERVER] La connessione con indice %d viene eliminata\n", ntohl(new_timer->packet->header.index));
                    fflush(stdout);
                    if (t_struct == NULL){
                        free(new_timer->packet);
                        free(new_timer->start_time);
                        free(new_timer->curr_timer);
                        free(new_timer);
                    }
                    else{
                        elimina_connessione(t_struct);
                        pthread_kill(t_struct->tid, SIGTERM);
                        delete_thread_struct(t_struct->thread_index);
                    }
                    
                }else{
                    //Se non si è raggiunto il numero di ritrasmissioni massimo riinvia il pacchetto
                    invia_pacchetto(new_timer->packet);
                    if(gettimeofday(new_timer->start_time, NULL) == -1){
                        error("[SERVER] Errore della funzione \"gettimeofday()\" in \"imposta_timer\"");
                    }
#ifdef TIMER_ADATTIVO
                    if (t_struct == NULL){//Connessione half established
                        //Il tempo raddoppia
                        long long int temp = tempo(new_timer->curr_timer);
                        new_timer->curr_timer->tv_sec = temp / ((long long int) pow(10,6));
                        new_timer->curr_timer->tv_usec = temp % ((long long int) pow(10,6)); 
                    }
                    else{
                        *(new_timer->curr_timer) = *(t_struct->timeout_adattivo);
                    }
#endif
                    add_timer(new_timer);
                }
            }
        }
        else{
            printf("[SERVER] Imposto il timer relativo al pacchetto con n_seq = %d\n", ntohl(timer_list.first->packet->header.n_seq));
            if(setitimer(ITIMER_REAL, &detect, NULL) == -1){
                error("[SERVER] Errore nell'impostazione del timer in \"imposta_timer\"");
            }
            break;
        }
    }
}

/**********************************************************************************************************
                                                 GESTIONE SIGALRM
**********************************************************************************************************/

//Questa funzione viene invocata quando il processo/thread viene colpito dal segnale SIGALRM
void handle_alarm(int sig){
    
    if (timer_list.first != NULL){
        printf("[SERVER] Inizio gestione handle_alarm()\n");
        fflush(stdout);

        struct sembuf buff;
        buff.sem_num = 0;
        buff.sem_op = -1;
        buff.sem_flg = 0;
        if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"handle_alarm\"");

        imposta_timer();

        buff.sem_op = +1;
        if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"handle_alarm\"");

        
        printf("[SERVER] Fine gestione handle_alarm()\n");
        fflush(stdout);
    }
    else{
        printf("Sto in handle alarm\n");
        fflush(stdout);
    }
}

/**********************************************************************************************************
                                                 GESTIONE ACK
**********************************************************************************************************/

//Questa funzione viene invocata quando arriva un segnale di ack
void handle_ack(nseq_t ack, thread_struct *t_struct, bool block){  
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
        
    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_flg = 0;
    
    if (block == true){
        //Blocca il segnale SIGALRM e richiede un token a sem_timer
        sigprocmask(SIG_BLOCK, &set, NULL);

        buff.sem_op = -1;
        if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"handle_ack\"");
    }

    bool first = false;
    printf("[SERVER] Inizio gestione handle_ack()\n");
    fflush(stdout);
    if(in_window(ack, t_struct->send_base, t_struct->send_limit_window)){
        //Se il primo elemento di timer_list è il timer da eliminare
        if (timer_list.first != NULL){
            if (indirizzo_uguale(&timer_list.first->packet->header.address, t_struct->address) && (ntohl(timer_list.first->packet->header.index) == t_struct->thread_index) && (ntohl(timer_list.first->packet->header.n_seq) == ack)){

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
        timer *ack_timer = remove_timer(t_struct->address, t_struct->thread_index, ack);
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
                timer_adattivo(sample_RTT, t_struct);
            } 
#endif
            if(ack_timer->packet->header.fin_flag == 1){
                delete_thread_struct(t_struct->thread_index);
            }

            free(ack_timer->packet);
            free(ack_timer->start_time);
            free(ack_timer->curr_timer);
            free(ack_timer);
        }

        //Se il timer da eliminare era il primo elemento di timer_list ma non l'unico imposta un nuovo timer
        if((first == true) && (timer_list.first != NULL)){ 
            printf("[SERVER] Reimposto il timer\n");
            fflush(stdout);
            imposta_timer();
        }
        
        //Aggiorna array_ack
        if(ack != t_struct->send_base){
            int pos;
            if(t_struct->send_base < ack){
                pos = ack - t_struct->send_base;
            }
            else{
                pos = (MAX_MSG_CLUSTER - t_struct->send_base) + ack;
            }
            (t_struct->array_ack)[pos] = 1;
        }
        else{
            //Determino la posizione del primo elemento diverso da 1
            int pos = 1;
            while(pos < WINDOW){
                if((t_struct->array_ack)[pos] == 1){
                    pos++;
                }
                else{
                    break;
                }
            }
            t_struct->send_base = (t_struct->send_base + pos) % MAX_MSG_CLUSTER;
            t_struct->send_limit_window = (t_struct->send_limit_window + pos) % MAX_MSG_CLUSTER;
            //Aggiorno array_ack
            for(int i = 0; i < (WINDOW - pos); i++){
                (t_struct->array_ack)[i] = (t_struct->array_ack)[pos+i];
            }
            for(int i = (WINDOW - pos); i < WINDOW; i++){
                (t_struct->array_ack)[i] = 0;
            }
        }
    }
    
    printf("[SERVER] Fine gestione handle_ack()\n");
    fflush(stdout);

    if (block == true){
        //Rilascia il token a sem_timer e sblocca i segnali
        buff.sem_op = +1;
        if (semop(sem_timer, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"handle_ack\"");

        sigprocmask(SIG_UNBLOCK, &set, NULL);
    }
}