# Progetto Sistemi Distribuiti e Cloud Computing

Storage chiave-valore distribuito, replicato e fortemente consistente implementato in Go.

## Componenti principali

- Consensus Node: nodo stateful che partecipa al protocollo di consenso.
- Client Proxy Service: servizio stateless che riceve le richieste dei client e le inoltra al leader.
- Snapshot & Backup Service: servizio stateless per snapshot e compattazione dei log.

## Tecnologie

- Go
- gRPC
- Protocol Buffers
- Docker
- Docker Compose
- AWS EC2

## Pattern architetturali

- Service Discovery
- Circuit Breaker

## Avvio locale

Da definire.
