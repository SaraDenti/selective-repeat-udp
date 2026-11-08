#pragma once

extern __thread thread_struct *t_struct;
extern char *subdirectory;

extern int sem_buffer_list;
extern int sem_rcv_list;

void send_new_packet(packet_t *packet);
bool crea_lista(void);
bool file_esistente(const char*filename);
void spedisci_file(packet_t *packet);
void ricevi_file(packet_t *packet);