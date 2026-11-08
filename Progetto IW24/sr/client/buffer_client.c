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

lista_buffer *crea_lista_buffer(void){

    lista_buffer *buffer_list = malloc(sizeof(lista_buffer));
    if(buffer_list == NULL){
        error("[CLIENT] Allocazione memoria fallita in \"crea_lista_buffer\"");
    }
    buffer_list->first = NULL;
    buffer_list->last = NULL;
    return buffer_list;
}

static buffer *crea_buffer(packet_t *packet){
    //Creo un buffer associato al pacchetto
    buffer *_buffer = malloc(sizeof(buffer));
    if(_buffer == NULL){
        error("[CLIENT] Allocazione memoria fallita in \"crea_buffer\"");
    }   
    _buffer->packet = packet;
    _buffer->next = NULL;

    return _buffer;
}

//Questa funzione viene invocata quando rcv_base != n_seq del pacchetto ricevuto.
//Ritorna true se il pacchetto è stato realmente inserito, false altrimenti
bool add_buffer(lista_buffer *buffer_list, packet_t *packet){
    if((buffer_list != NULL) && (packet != NULL)){
        if((ntohl(packet->header.n_seq) != rcv_base) && (in_window(ntohl(packet->header.n_seq), rcv_base, rcv_limit_window) == true)){
            buffer *old_buffer = NULL;
            buffer *new_buffer = buffer_list->first;
            
            nseq_t n_seq = ntohl(packet->header.n_seq); //numero di sequenza del pacchetto da inserire
            //Il numero del pacchetto da inserire è inferiore a rcv_base: si scorrono tutti gli elementi della lista
            //con numero > rcv_base per poi inserire il  relativo pacchetto tra i due pacchetti corretti
            if(n_seq < rcv_base){
                while((new_buffer != NULL) && (ntohl(new_buffer->packet->header.n_seq) > rcv_base)){
                    old_buffer = new_buffer;
                    new_buffer = new_buffer->next;
                }
                while((new_buffer != NULL) && (n_seq > ntohl(new_buffer->packet->header.n_seq))){
                    old_buffer = new_buffer;
                    new_buffer = new_buffer->next;
                }
            }
            //Il numero del pacchetto da inserire è maggiore a rcv_base: si scorrono tutti gli elementi della lista
            //con numero > rcv_base cercando il posto opportuno in cui inserire il pacchetto. Se gli elementi con
            //numero > rcv_base terminano il pacchetto viene inserito alla fine, indipendentemente se vi siano altri elementi
            else{
                while((new_buffer != NULL) && (ntohl(new_buffer->packet->header.n_seq) > rcv_base) && (n_seq > ntohl(new_buffer->packet->header.n_seq))){
                    old_buffer = new_buffer;
                    new_buffer = new_buffer->next;
                }
            }
           if((new_buffer != NULL) && (n_seq == ntohl(new_buffer->packet->header.n_seq))){
                    printf("[CLIENT] Scarto il pacchetto duplicato\n");
                    fflush(stdout);
                    free(packet);
                    return false;
            }
            else{ //Il pacchetto non è un duplicato
                buffer *_buffer;
                if(old_buffer == NULL){ //Il nuovo pacchetto è il primo elemento della lista
                    //Crea un buffer associato al pacchetto e lo inserisce all'interno della lista
                    _buffer = crea_buffer(packet);
                    buffer_list->first = _buffer;
                    _buffer->next = new_buffer;
                }
                else{
                    //Crea un buffer associato al pacchetto e lo inserisce all'interno della lista
                    buffer *_buffer = crea_buffer(packet);
                    old_buffer->next = _buffer;
                    _buffer->next = new_buffer;
                }
                if (buffer_list->last == old_buffer){
                    buffer_list->last = _buffer;
                }
                return true;
            }
        }
    }
    return false;
}

//Questa funzione viene invocata quando rcv_base == n_seq del pacchetto ricevuto
lista_buffer *remove_buffer(lista_buffer *buffer_list, packet_t *packet){

    if((buffer_list != NULL) && (packet != NULL)){
        if (ntohl(packet->header.n_seq) == rcv_base){

            //Creo una nuova lista di buffer che verrà passata al livello superiore
            lista_buffer *new_list = crea_lista_buffer();
            buffer *_buffer = crea_buffer(packet);
            new_list->first = _buffer;
            new_list->last = _buffer;

            //Aggiungo eventuali pacchetti consecutivi alla lista
            nseq_t num_seq = ntohl(packet->header.n_seq);

            buffer *old_buffer = NULL;
            buffer *next_buffer = buffer_list->first;

            while((next_buffer != NULL) && (ntohl(next_buffer->packet->header.n_seq) == ((num_seq + 1) % MAX_MSG_CLUSTER))){
                old_buffer = next_buffer;
                next_buffer = next_buffer->next;
                num_seq = (num_seq + 1) % MAX_MSG_CLUSTER;
            }

            if(old_buffer != NULL){ //E' possibile aggiungere altri elementi alla lista
                _buffer->next = buffer_list->first;
                old_buffer->next = NULL;
                new_list ->last = old_buffer;
                rcv_base = (ntohl(old_buffer->packet->header.n_seq) +1) % MAX_MSG_CLUSTER;
                rcv_limit_window = (rcv_base + WINDOW - 1) % MAX_MSG_CLUSTER;
            }
            else{//Alla lista non sono stati aggiunti altri elementi oltre packet
                rcv_base = (ntohl(packet->header.n_seq) +1) % MAX_MSG_CLUSTER;
                rcv_limit_window = (rcv_base + WINDOW - 1) % MAX_MSG_CLUSTER;
            }
            buffer_list->first = next_buffer;
            return new_list;
        }
        else{
            return NULL;
        }
    }
    else{
        return NULL;
    }
}