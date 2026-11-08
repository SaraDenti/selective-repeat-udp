#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <dirent.h>
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

#include "header.h"

#include "../shared/config.h"
#include "../shared/packet.h"


// ------------------------------------------- FUNZIONE D'ERRORE -------------------------------------------
void error(char *s){
    perror(s);
    fflush(stderr);
    if(opened == true){
        while(close(client_socket) == -1){
            if(errno == EINTR){
                errno = 0;
            }else{
                fprintf(stderr, "[CLIENT] Errore della funzione \"close\" in \"error\"\n");
                fflush(stderr);
                break;
            }
        }
    }
    exit(EXIT_FAILURE);
}

// -------------------------------------------- INVIO PACCHETTI --------------------------------------------

static float loss_probability = 0.05; //5% di probabilità di perdita

//Tale funzione ritorna true se il pacchetto viene perso, false altrimenti
static bool simulate_loss(float p){
    return ((float)rand() / RAND_MAX) < p;
}

void invia_pacchetto(packet_t *packet){
    if(simulate_loss(loss_probability) == false){
        if (packet->header.ack_flag == 0){
            printf("[CLIENT] Inviando un pacchetto di %d bytes (n_seq = %d)\n", ntohl(packet->header.len), ntohl(packet->header.n_seq));
            fflush(stdout);
        }
        else{
            printf("[CLIENT] Inviando un pacchetto di ACK di %d bytes (ack = %d)\n", ntohl(packet->header.len), ntohl(packet->header.ack));
            fflush(stdout);
        }
        ssize_t byte_scritti = 0;
        ssize_t ret;
        while(byte_scritti < (ssize_t)ntohl(packet->header.len)){
            ret = sendto(client_socket, &packet[byte_scritti], ntohl(packet->header.len) - byte_scritti, MSG_CONFIRM, NULL, 0);
            if(ret == -1){
                if(errno == EINTR) errno = 0;
                else error("[CLIENT] Errore della funzione \"sendto()\" in \"invia_pacchetto\"");
            }
            else{
                byte_scritti += ret;
            }
        }
    }
    else{
        if(packet->header.ack_flag == 0){
            printf("[CLIENT] Pacchetto perso (n_seq = %d)\n", ntohl(packet->header.n_seq));
            fflush(stdout);
        }
        else{
            printf("[CLIENT] Pacchetto perso (ack = %d)\n", ntohl(packet->header.ack));
            fflush(stdout);
        }
    }
}

// ------------------------------------------ RICEZIONE PACCHETTI ------------------------------------------

void ricevi_pacchetto(packet_t *packet){
    memset(packet, '\0', MAX_PACKET_LEN);

    ssize_t byte_letti = 0;
    ssize_t ret;
    while(byte_letti < MAX_PACKET_LEN){
        ret = recvfrom(client_socket, &packet[byte_letti], MAX_PACKET_LEN - byte_letti, 0, NULL, NULL);
        if(ret == -1){
            if(errno == EINTR) errno = 0;
            else error("[CLIENT] Errore della funzione \"rcvfrom()\" in \"ricevi_pacchetto\"");
        }
        else{
            byte_letti += ret;
            if ((long unsigned int) byte_letti >= sizeof(packet_header_t)){
                if ((long unsigned int)byte_letti == ntohl(packet->header.len)){
                    break;
                } 
            }
        }
    }
}


// ---------------------------------- FINESTRA DI RICEZIONE/TRASMISSIONE -----------------------------------

//Questa funzione verifica se nseq appartiene alla finestra (di ricezione o di trasmissione)
bool in_window(nseq_t nseq, nseq_t base, nseq_t limit){
    if(base <= limit){
        if((nseq >= base) && (nseq <= limit)){
            return true;
        }
    }
    else{
        if(((nseq >= base) && (nseq < MAX_MSG_CLUSTER)) || (nseq <= limit)){
            return true;
        }
    }
    return false;
}