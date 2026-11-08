# Selective Repeat File Transfer over UDP

A client–server file transfer application written in C using Berkeley sockets. It implements Selective Repeat over UDP, allowing the sender to retransmit individual unacknowledged packets while the receiver buffers packets that arrive out of order.

## Features

- List files available on the server, download files, and upload files.
- Handle multiple client connections with a multithreaded server.
- Manage transfers with a sliding window, per-packet acknowledgements, and retransmission timers.
- Choose between a fixed timeout and an adaptive timeout based on round-trip time (RTT) measurements.

## Build and run

You need a C compiler, `make`, and a POSIX environment with Berkeley sockets and POSIX threads.

From the directory containing the `Makefile`, build with the default fixed timeout:

```sh
make
```

To switch to the adaptive timeout, rebuild with:

```sh
make clean
make TIMER=TIMER_ADATTIVO
```

Start the server in one terminal from the directory containing the `Makefile`:

```sh
cd server
./server
```

Start the client in a second terminal from the same starting directory:

```sh
cd client
./client
```

Use the client menu to list available files, download a file, upload a file, or exit.

## Project structure

| Path | Purpose |
| --- | --- |
| `client/` | Client application, packet handling, buffers, and timers |
| `server/` | Server application, connection management, and file handling |
| `shared/` | Shared packet definitions and protocol configuration |
| `Makefile` | Builds both programs and selects the timeout mode |
| [`report.pdf`](report.pdf) | Protocol design, implementation details, and performance analysis |

## Performance analysis

The [project report](report.pdf) examines how file transfer time varies with the maximum segment size (MSS), retransmission timeout, and send window size. It also compares fixed and adaptive timeouts in the scenarios studied.

## Authors

Luca Callegari, Mirko Damiano, and Sara Denti. Developed as a group project for the Internet and Web Engineering course at the University of Rome Tor Vergata.
