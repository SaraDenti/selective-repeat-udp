#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <netinet/in.h>

#include "config.h"

#define CMD_LIST 1
#define CMD_GET 2
#define CMD_PUT 3

typedef uint16_t packet_type_t;
typedef uint32_t packet_len_t;
typedef uint16_t cmd_t; //CMD idenfitica il comando relativo al pacchetto
typedef uint32_t pos_t; //valore del primo byte
typedef uint32_t nseq_t; //valore del numero di sequenza
typedef uint32_t index_t;

typedef struct _packet_header_t {
    char syn_flag;
    char rst_flag;
    char fin_flag;
    index_t index;

    nseq_t n_seq;
    nseq_t ack;
    char ack_flag; //flag che vale 1 se il pacchetto è un ACK, 0 altrimenti
    
    cmd_t cmd;
    pos_t pos;
    char end_flag; //variabile per indicare se e l'ultimo pacchetto del flusso dati
    char error_flag; // flag per verificare se un file richiesto dal client esista o meno (vale 1 se il file non è presente, vale 0 viceversa)

    packet_len_t len;
    packet_len_t body_len;
    
    struct sockaddr_in address; //indirizzo del socket destinazione

} packet_header_t;

typedef struct _packet_t {
    packet_header_t header;
    char filename [MAX_FILENAME_LENGTH];
    char body[];
} packet_t;

packet_t *make_packet(char _syn_flag, char _rst_flag, char _fin_flag, index_t _index, cmd_t _cmd, pos_t _pos, char _end_flag, char _error_flag, nseq_t _n_seq, char *_filename, packet_len_t _filename_len, char *_body, packet_len_t _body_len, nseq_t _ack, char _ack_flag);
