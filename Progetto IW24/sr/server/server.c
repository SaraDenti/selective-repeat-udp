#include <stdio.h>
#include <errno.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/sem.h>

#include "buffer_server.h"
#include "header.h"
#include "server.h"

char* subdirectory = "file_disponibili";//Directory in cui verranno salvati tutti i file disponibili per la condivisione

/****************************************************************************************************************************************
                                                            DOWNLOAD E LIST
*****************************************************************************************************************************************/

//Questa funzione inserisce il file "filename.txt". Ritorna:
// true se sono disponibili file per la condivisione
// false se non sono disponibili file per la condivisione
bool crea_lista(void){
    const char *target_directory = "file_disponibili";
    
    // Apre il file di output in modalità scrittura
    FILE *output_file = fopen("filename.txt", "w");
    if (output_file == NULL){
        error("[SERVER] Errore nell'apertura del file 'filename.txt' in \"crea_lista\"");
    }

    // Apre la directory target
    DIR *directory = opendir(target_directory);
    if (directory == NULL) {
        if (errno == ENOENT){//La directory non esiste
        	printf("[THREAD %lu] Non sono disponibili file per la condivisione\n", pthread_self());
        	fflush(stdout);
        	return false;
        }
        else{ 
            fclose(output_file);
            error("[SERVER] Errore nell'apertura della directory in \"crea_lista\"");
        }
    }

    struct dirent *entry;
    
    bool file_disponibili = false;

    // Legge i contenuti della directory
    while ((entry = readdir(directory)) != NULL) {
        // Controlla se l'entry è un file normale
        if (entry->d_type == DT_REG) {
            // Scrive il nome del file nel file di output
            if (file_disponibili == false){
            	file_disponibili = true;
            }
            fprintf(output_file, "%s\n", entry->d_name);
            fflush(output_file);
        }
    }

    // Chiude la directory e il file di output
    closedir(directory);
    fclose(output_file);

    if (file_disponibili == true){
        printf("[THREAD %lu] Elenco dei file salvato in filename.txt\n", pthread_self());
        fflush(stdout);
    }
    else{
        printf("[THREAD %lu] Nessun file disponibile\n", pthread_self());
        fflush(stdout);
    }

    return file_disponibili;
}

void spedisci_file(packet_t *packet){ 

    // Apro il file
    FILE *f;
    if (ntohs(packet->header.cmd) == CMD_LIST){
    	f = fopen(packet->filename, "r");
    }
    else{//Il file si trova nella sottodirectory subdirectory
    	char percorso_completo[MAX_PATHNAME_LENGTH]; 
    	if (snprintf(percorso_completo, MAX_PATHNAME_LENGTH, "%s/%s", subdirectory, packet->filename) < 0) error("[SERVER] Errore della funzione \"snprintf()\" in \"spedisci_file\"");
    	f = fopen(percorso_completo, "r");
    }
    if(f == NULL) error("[SERVER] Errore apertura file in \"spedisci_file\"");
    
    // Buffer per inviare il messaggio
    void *buffer = malloc(MAX_PACKET_BODY_LEN);
    if (buffer == NULL) error("[SERVER] Allocazione memoria fallita in \"spedisci_file\"");

    char end_flag = 0;
    packet_t *new_packet;
    do {
        memset(buffer, 0, MAX_PACKET_BODY_LEN);

        size_t byte_letti = fread(buffer, sizeof(char), MAX_PACKET_BODY_LEN - 1, f);
        if (byte_letti < MAX_PACKET_BODY_LEN - 1){ //(la funzione non viene mai bloccata da un segnale)
            if (feof(f)){
                end_flag = 1;
            }
            else if(ferror(f)){
                error("[SERVER] Errore della funzione \"fread()\" in \"spedisci_file\"");
            }
        }
        
        if (packet->header.cmd == CMD_LIST){
            new_packet = make_packet(0, 0, 0, 0, CMD_LIST, 0, end_flag, 0, 0, packet->filename, strlen(packet->filename) + 1, buffer, byte_letti + 1, 0, 0);
        }
        else{
        	new_packet =  make_packet(0, 0, 0, 0, CMD_GET, 0, end_flag, 0, 0, packet->filename, strlen(packet->filename) + 1, buffer, byte_letti + 1, 0, 0);
        }
        send_new_packet(new_packet);

    } while(!feof(f));

    printf("\n[SERVER] Download di %s completato\n", packet->filename); 
    fflush(stdout);
    fclose(f);
    free(buffer); 
}


bool file_esistente(const char*filename){
    struct dirent *entry;
    
    DIR *dir = opendir(subdirectory);

    if (dir == NULL) {
        perror("[SERVER] Errore nell'apertura della directory");
        return false; // File non trovato
    }

    while ((entry = readdir(dir)) != NULL) {
        // Ignora "." e ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        else if (strcmp(entry->d_name, filename) == 0) {
                // File trovato
                closedir(dir);
                return true;
        }
    }

    closedir(dir);
    return false; // File non trovato
}

/****************************************************************************************************************************************
                                                                        UPLOAD
*****************************************************************************************************************************************/

void ricevi_file(packet_t *packet){
    char *filename = packet->filename;
    
    // Costruisce il percorso completo
    int len = snprintf(NULL, 0, "%s/%s", subdirectory, filename);
    if (len < 0) error("[SERVER] Errore della funzione \"snprintf()\" in \"ricevi_file\"");
    char file_path[len + 1];
    if (sprintf(file_path, "%s/%s", subdirectory, filename) < 0) error("[SERVER] Errore della funzione \"snprintf()\"(1) in \"ricevi_file\"");
    file_path[len] = '\0';

    //Apre il file
    FILE *f = fopen(file_path, "w");
    if (f == NULL){
        error("[SERVER] Errore della funzione \"fopen()\" in \"ricevi_file\"");
    }
    printf("[THREAD %ld] Download del file %s dal client\n", pthread_self(), packet->filename);
    fflush(stdout);

    //Scrive nel file il primo pacchetto arrivato
    fprintf(f, "%s", packet->body);
    fflush(f);

    char end_flag = packet->header.end_flag;

    free(packet);

    //Se il file è composto da più di un pacchetto, aspetto di ricevere gli altri pacchetti e li salvo nel file
    struct buffer *temp_buff;
 
    while(!end_flag){
        //Aspetto che ci sia almeno un pacchetto nel buffer
        while(t_struct->buffer_list->first == NULL);

        struct sembuf buff;
        buff.sem_num = 0;
        buff.sem_op = -1;
        buff.sem_flg = 0;
        if (semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\" in \"ricevi_file\"");

        //Rimuovo il primo pacchetto da buffer_list
        temp_buff = t_struct->buffer_list->first;
 
        if(t_struct->buffer_list->first == t_struct->buffer_list->last){ //Buffer list è composta da solo un elemento
            t_struct->buffer_list->first = NULL;
            t_struct->buffer_list->last = NULL;
        }else{
            t_struct->buffer_list->first = t_struct->buffer_list->first->next;
        }
        packet = temp_buff->packet;
        free(temp_buff); 

        buff.sem_op = +1;
        if (semop(sem_buffer_list, &buff, 1) == -1) error("[SERVER] Errore della funzione \"semop()\"(1) in \"ricevi_file\"");
         
        //Scrive nel file f il body del pacchetto
        fprintf(f, "%s", packet->body);
        fflush(f);
         
        //Verifico se il file in download è stato completamente ricevuto
        end_flag = packet->header.end_flag;
 
        free(packet);
 
    }
    printf("[SERVER] Download del file %s completato\n", filename); 
    fflush(stdout);
 
    fclose(f);
}