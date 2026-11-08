#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <arpa/inet.h>
#include <stdio.h>

#include "config.h"
#include "packet.h"

#ifdef CLIENT
#include "../client/header.h"
#elif defined(SERVER)
#include "../server/header.h"
#endif


packet_t *make_packet(char _syn_flag, char _rst_flag, char _fin_flag, index_t _index, cmd_t _cmd, pos_t _pos, char _end_flag, char _error_flag, nseq_t _n_seq, char *_filename, packet_len_t _filename_len, char *_body, packet_len_t _body_len, nseq_t _ack, char _ack_flag){
    packet_len_t packet_len = sizeof(packet_header_t) + MAX_FILENAME_LENGTH + _body_len;

    // Alloca il pacchetto
    packet_t *packet = malloc(packet_len);
    if (!packet) {
        error("[MAIN] Allocazione memoria fallita in \"make_packet\"");
    }
    memset(packet, 0, packet_len);

    //Associo ad ogni elemento della struttura il valore corrispondente

    packet->header.syn_flag = _syn_flag;
    packet->header.rst_flag = _rst_flag;
    packet->header.fin_flag = _fin_flag;
    packet->header.index = htonl(_index);

    packet->header.n_seq = htonl(_n_seq);
    packet->header.ack = htonl(_ack);
    packet->header.ack_flag = _ack_flag;

    packet->header.cmd = htons(_cmd);
    packet->header.pos = htonl(_pos); 
    packet->header.end_flag = _end_flag;
    packet->header.error_flag = _error_flag;

    packet->header.len = htonl(packet_len);
    packet->header.body_len = htonl(_body_len);
    
    memcpy(packet->filename, _filename, _filename_len);
    memcpy(packet->body, _body, _body_len);
    return packet;
}