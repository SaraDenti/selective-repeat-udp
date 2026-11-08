#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/sem.h>
#include <sys/socket.h>
#include <pthread.h>
#include <unistd.h>
#include <dirent.h>

#include "header.h"
#include "SRclient.h"

int sem_buffer_list;
int sem_rcv_list;

pthread_t tid; //TID livello 2

static void leggi_stringa(char *prompt, char **input){
    printf("%s", prompt);
    fflush(stdout);
    while(1){
        if(scanf("%ms", input) != 1) error("[CLIENT] Errore della funzione \"scanf()\" in \"leggi_stringa\"");
        if((strlen(*input) > MAX_FILENAME_LENGTH - 1) || (strchr(*input, '/'))){
            printf("[CLIENT] Il nome del file non è valido! Inserirne un altro:\t");
            fflush(stdout);
            while(getchar() != '\n');
            free(input);
        }
        else{
            break;
        }
    }
    while(getchar() != '\n');
}
/****************************************************************************************************************************************
                                                            UPLOAD
*****************************************************************************************************************************************/

//Questa funzione ritorna 1 se il file client_filename è presente nella cartella corrente; ritorna 0 altrimenti
static int file_presente_nel_client(char *client_filename){
    struct dirent *de;  // Struttura per contenere le informazioni sui file

    // Aprire la directory corrente "."
    DIR *dr = opendir(".");

    if(dr == NULL){  // Se la directory non puo essere aperta
        error("[CLIENT] Errore nell'apertura della directory");
    }

    while((de = readdir(dr)) != NULL){
        if(!strcmp(de->d_name, client_filename)){
            // Chiudere la directory
            closedir(dr);
            return 1;
        }
    }
    // Chiudere la directory
    closedir(dr);
    return 0;
}

void spedisci_file(char *client_filename){ 

    // Apro il file
    FILE *f = fopen(client_filename, "r");
    if(f == NULL) error("[CLIENT] Errore apertura file in \"spedisci_file\"");
    
    // Buffer per inviare il messaggio
    char *buffer = malloc(MAX_PACKET_BODY_LEN);
    if(buffer == NULL) error("[CLIENT] Allocazione memoria fallita in \"spedisci_file\"");

    char end_flag = 0;
    packet_t *new_packet;
    do {
        memset(buffer, 0, MAX_PACKET_BODY_LEN);

        size_t byte_letti = fread(buffer, sizeof(char), MAX_PACKET_BODY_LEN - 1, f);
        if(byte_letti < MAX_PACKET_BODY_LEN - 1){ //(la funzione non viene mai bloccata da un segnale)
            if (feof(f)){
                end_flag = 1;
            }
            else if(ferror(f)){
                error("[CLIENT] Errore della funzione \"fread()\" in \"spedisci_file\"");
            }
        }
        
        new_packet = make_packet(0, 0, 0, 0, CMD_PUT, 0, end_flag, 0, 0, client_filename, strlen(client_filename) + 1, buffer, byte_letti + 1, 0, 0);
        if(handshake == false){ //Connessione non instaurata
            //Creazione ed invio al server del pacchetto che richiede il file "filename.txt"
            if(pthread_create(&tid, NULL, apri_connessione, (void*)new_packet) != 0){
                error("[CLIENT] Errore nella creazione del thread in \"spedisci_file\"");
            } 
            while(handshake == false);
        }
        else{
            send_new_packet(new_packet);
        }

    } while(!feof(f));

    printf("\n[CLIENT] Upload di %s completato\n", client_filename); 
    fflush(stdout);
    fclose(f);
    free(buffer); 
}

/****************************************************************************************************************************************
                                                            DOWNLOAD E LIST
*****************************************************************************************************************************************/

//questa funzione restituisce true se il file è stato ricevuto senza errori; restituisce false se uno tra i pacchetti ricevuti a error_flag impostato a 1 o se si è verificato un errore nella ricezione del file
static bool ricevi_file(void){

   // --- Salvo il file ---

    char end_flag = 0;

    packet_t *packet;

    struct buffer *temp_buff;

    bool first = true;
    FILE *f;

    struct sembuf buff;
    buff.sem_num = 0;
    buff.sem_flg = 0;

    while(!end_flag){
        //Aspetto che ci sia almeno un pacchetto nel buffer
        while(buffer_list->first == NULL);

        buff.sem_op = -1;
        if(semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\" in \"ricevi_file\"");

        //Rimuovo il primo pacchetto da buffer_list
        temp_buff = buffer_list->first;

        if(buffer_list->first == buffer_list->last){
            buffer_list->first = NULL;
            buffer_list->last = NULL;
        }else{
            buffer_list->first = buffer_list->first->next;
        }
        packet = temp_buff->packet;
        free(temp_buff); 

        buff.sem_op = +1;
        if(semop(sem_buffer_list, &buff, 1) == -1) error("[CLIENT] Errore della funzione \"semop()\"(1) in \"ricevi_file\"");

        
        if(first == true){
            if(packet->header.error_flag == 1){
                printf("[SERVER] %s", packet->body);
                fflush(stdout);
                free(packet);
                return false;
            }
            else{
                //Apre il file
                f = fopen(packet->filename, "w");
                if(f == NULL){
                    error("[CLIENT] Errore della funzione \"fopen()\" in \"ricevi_file\"");
                }
                printf("[CLIENT] Download del file %s dal server...\n", packet->filename);
                fflush(stdout);
            }
        }

        //Scrive nel file f il body del pacchetto
        fprintf(f, "%s", packet->body);
        fflush(f);
        
        //verifico se il file in download è stato completamente ricevuto
        end_flag = packet->header.end_flag;
        if(end_flag) printf("[CLIENT] Download di %s dal server completato\n", packet->filename); 

        free(packet);
    }
    fclose(f);

    return true;
}


int main(void){
    
    packet_t *packet;
    char *client_filename = NULL;

    printf("\n\n\tBenvenuto nel server\n\n\n\n");

    //Blocca tutti i segnali
    sigset_t set;
    sigfillset(&set);
    sigdelset(&set, SIGINT); //DA ELIMINARE
    sigprocmask(SIG_BLOCK, &set, NULL);

    while(1){

        printf("\n---------MENU-----------\n\n");
        printf("\t1. Lista file disponibili per la condivisione \n");
        printf("\t2. Download file \n");
        printf("\t3. Upload file \n");
        printf("\t4. Termina il programma\n");
        printf("\n----------------------------------------\n");

        printf("\nInserisci la tua scelta: ");
        char scelta = getchar();
        while(getchar() != '\n'); 

        switch(scelta){
            case '1': //LISTA
                packet = make_packet(0, 0, 0, 0, CMD_LIST, 0, 1, 0, 0, "filename.txt", strlen("filename.txt") + 1, NULL, 0, 0, 0); 
                    
                if(handshake == false){ //Connessione non instaurata
                    //Creazione ed invio al server del pacchetto che richiede il file "filename.txt"
                    if(pthread_create(&tid, NULL, apri_connessione, (void*)packet) != 0){
                        error("[CLIENT] Errore nella creazione del thread in \"main\"");
                    } 
                    while(handshake == false);
                }
                else{
                    send_new_packet(packet);
                }
                //Ricezione file giusto
                if(ricevi_file()){
                    //Stampo su standard output il contenuto del file
                    printf ("[CLIENT] I file disponibili per la condivisione sono:\n");
                    system("cat filename.txt");
                    fflush(stdout);
                    system("rm filename.txt");
                }
                break;
            case '2': //DOWNLOAD
                leggi_stringa("Inserire il nome del file: ", &client_filename);
                printf("[CLIENT] Downloading di %s...\n", client_filename);
                fflush(stdout);

		        //Creazione ed invio al server del pacchetto che richiede il file client_filename
                packet = make_packet(0, 0, 0, 0, CMD_GET, 0, 1, 0, 0, client_filename, strlen(client_filename) + 1, NULL, 0, 0, 0);
                if(handshake == false){ //Connessione non instaurata
                    //Creazione ed invio al server del pacchetto che richiede il file "filename.txt"
                    if(pthread_create(&tid, NULL, apri_connessione, (void*)packet) != 0){
                        error("[CLIENT] Errore nella creazione del thread(1) in \"main\"");
                    } 
                    while(handshake == false);
                }
                else{
                    send_new_packet(packet);
                }
                
                //Recezione file giusto
                ricevi_file();
    
                break;
            case '3': //UPLOAD
                leggi_stringa("Inserire il nome del file: ", &client_filename);
                printf("[CLIENT] Caricando il file %s...\n", client_filename);
                fflush(stdout);
                if(file_presente_nel_client(client_filename)) {
                    printf("[CLIENT] File %s presente\n", client_filename);
                    fflush(stdout);
                    spedisci_file(client_filename);
                }else{
                    fprintf(stderr, "[CLIENT] File %s non presente\n", client_filename);
                    fflush(stderr);
                }
                break;
            case '4': //TERMINAZIONE
                if(handshake == true){
                    //Aspetta di aver ricevuto dal server tutti gli ack 
                    while(timer_list.first != NULL);
                    
                    //Invia pacchetto con fin_flag = 1
                    packet_t *fin_packet = make_packet(0, 0, 1, 0, 0, 0, 1, 0, 0, NULL, 0, NULL, 0, 0, 0);
                    send_new_packet(fin_packet);
                    pthread_join(tid, NULL);

                    printf("[CLIENT] Il programma è terminato\n");
                    fflush(stdout);
                }else{
                    printf("[CLIENT] La connessione non è stata ancora stabilita\n");
                    fflush(stdout);
                }
                exit(EXIT_SUCCESS);
                break;
        }   
    }

    close(client_socket);
    exit(EXIT_SUCCESS);
}