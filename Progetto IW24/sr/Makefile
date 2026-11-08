# Makefile nella directory principale
CC = gcc
CFLAGS = -Wall -Wextra -Ishared -Iclient -Iserver

# Timer di default (può essere sovrascritto da riga di comando)
TIMER ?= TIMER_FISSO

# Sorgenti per il client
CLIENT_SRCS = client/client.c client/SRclient.c client/header.c client/buffer_client.c client/timer_client.c shared/packet.c 

# Sorgenti per il server
SERVER_SRCS = server/server.c server/SRserver.c server/header.c server/timer_server.c server/buffer_server.c shared/packet.c 

.PHONY: all clean

# Compilazione di entrambi gli eseguibili
all: client/client server/server

# Eseguibile client
client/client: $(CLIENT_SRCS)
	$(CC) $(CFLAGS) -g -DCLIENT -D$(TIMER) -o client/client $(CLIENT_SRCS)

# Eseguibile server
server/server: $(SERVER_SRCS)
	$(CC) $(CFLAGS) -g -DSERVER -D$(TIMER) -o server/server $(SERVER_SRCS)

# Pulizia degli eseguibili
clean:
	rm -f client/client server/server
