#pragma once
#include <stdbool.h>

#include "buffer_client.h"

extern int client_socket; //ID del socket del client
extern lista_buffer *rcv_list; //Variabile globale che indica il buffer dei pacchetti ricevuti ma non ancora passati al livello 1
extern lista_buffer *buffer_list; //variabile globale che indica il buffer d'interfaccia tra livello 1 e livello 2

extern bool opened;
void error(char *s);

void invia_pacchetto(packet_t *packet);
void ricevi_pacchetto(packet_t *packet);

bool in_window(nseq_t nseq, nseq_t base, nseq_t limit);