#pragma once

#include "timer_client.h"

extern int sem_buffer_list;
extern int sem_rcv_list;

extern bool handshake;
extern lista_timer timer_list;

extern index_t thread_index;
void *apri_connessione(void *arg);
void send_new_packet(packet_t *packet);