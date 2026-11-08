#pragma once

#include "header.h"

#include "../shared/packet.h"

typedef struct buffer{
    packet_t *packet;
    struct buffer *next;
} buffer;

typedef struct lista_buffer{
    buffer *first;
    buffer *last;
} lista_buffer;

lista_buffer *crea_lista_buffer(void);
bool add_buffer(lista_buffer *buffer_list, packet_t *packet, thread_struct *t_struct);
lista_buffer *remove_buffer(lista_buffer *buffer_list, packet_t *packet, thread_struct *t_struct);