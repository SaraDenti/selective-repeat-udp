#pragma once

typedef struct timer{
    packet_t *packet;
    struct timeval *start_time; //momento in cui viene avviato il timer
    struct timeval *curr_timer; //durata del timer
    struct timer *next; //prossimo elemento della lista
    int count; //Numero di volte in cui è scaduto il timer
} timer;

typedef struct lista_timer{
    timer *first;
    timer *last;
} lista_timer;

extern lista_timer timer_list; //variabile globale che indica la lista dei timer attivi

extern int sem_timer;

timer *create_timer(packet_t *packet, struct timeval *timer_da_impostare);
void add_timer(struct timer *new_timer);

timer *remove_timer(struct sockaddr_in *address, index_t index, nseq_t _nseq);

void handle_alarm(int sig);
void handle_ack(nseq_t ack, thread_struct *t_struct, bool block);