//Porta sulla quale il server è in ascolto
#define SERVER_PORT 8080
//Lunghezza massima del path e del filename
#define MAX_PATHNAME_LENGTH 4096 
#define MAX_FILENAME_LENGTH 256 
//Finestra di trasmissione/ricezione
#define MAX_MSG_CLUSTER 50
#define WINDOW 10
#define MAX_COUNT 5 
//Lunghezze pacchetti
#define MAX_PACKET_LEN 1024
#define MAX_PACKET_BODY_LEN (MAX_PACKET_LEN - sizeof(packet_header_t) - MAX_FILENAME_LENGTH)
//Multithreading
#define WORKER_THREADS_NUM 5
//Timer adattivo
#define ALPHA 0.125
#define BETA 0.25
#define TIMER_MIN 100000