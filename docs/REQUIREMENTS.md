# Requisiti e dipendenze del progetto

Questo progetto è sviluppato in Go. Per questo motivo non usa un file `requirements.txt` come nei progetti Python.

Il punto di verità per le dipendenze applicative Go è:

```text
go.mod
```

Il file:

```text
go.sum
```

contiene invece i checksum delle versioni dei moduli risolti, utili per build riproducibili.

---

## 1. Versione Go

Versione minima richiesta:

```text
Go >= 1.25.4
```

Verifica:

```bash
go version
```

---

## 2. Dipendenze Go principali

Le dipendenze sono gestite da `go.mod`.

Dipendenze rilevanti:

```text
google.golang.org/grpc
  Comunicazione gRPC tra Consensus Node, Client Proxy e Backup Service.

google.golang.org/protobuf
  Serializzazione dei messaggi definiti nei file .proto.

github.com/sony/gobreaker/v2
  Circuit Breaker nel Client Proxy e nel Backup Service.

github.com/stretchr/testify
  Libreria di supporto per test più leggibili.
```

Comando di manutenzione:

```bash
go mod tidy
```

Comando di verifica:

```bash
go test ./...
```

---

## 3. Tool Protobuf

Per rigenerare gli stub Go dai file `.proto` servono:

```text
protoc
protoc-gen-go
protoc-gen-go-grpc
```

Versioni usate nella CI:

```text
protoc             33.0
protoc-gen-go      v1.36.10
protoc-gen-go-grpc v1.5.1
```

Installazione plugin Go:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```

Rigenerazione stub:

```bash
make proto
```

---

## 4. Buf

Buf viene usato per:

```text
- lint dei file .proto;
- format check;
- breaking change check nelle pull request.
```

Verifica installazione:

```bash
buf --version
```

Comandi:

```bash
buf lint
buf format --diff --exit-code
```

Formattazione automatica:

```bash
buf format -w proto
```

Nota Windows: se Buf richiede `diff`, può essere necessario avere `diff.exe` nel PATH, ad esempio tramite Git for Windows.

---

## 5. Docker

Per eseguire il cluster containerizzato servono:

```text
Docker Engine
Docker Compose plugin
Docker Buildx
```

Verifica:

```bash
docker --version
docker compose version
docker buildx version
```

Su EC2 Learner Lab è stato necessario aggiornare Buildx perché la versione iniziale era troppo vecchia:

```text
versione iniziale: 0.12.1
versione installata: 0.35.0
```

Errore risolto:

```text
compose build requires buildx 0.17.0 or later
```

---

## 6. Dipendenze runtime nei container

Le immagini finali Alpine installano solo ciò che serve.

Consensus Node:

```text
ca-certificates
```

Client Proxy:

```text
ca-certificates
wget
```

Backup Service:

```text
ca-certificates
```

`wget` è necessario nel container del Client Proxy perché Docker Compose usa:

```text
wget -qO- http://localhost:8081/health
```

per l'healthcheck.

---

## 7. Requisiti EC2 Learner Lab

Ambiente validato:

```text
AWS Academy Learner Lab
Region: us-east-1
AMI: Amazon Linux 2023
Instance type: t3.small
Key pair: vockey
SSH key nel terminale browser: ~/.ssh/labsuser.pem
```

Porte Security Group usate:

```text
22/tcp    SSH
8080/tcp  Client Proxy gRPC
8081/tcp  Client Proxy /health
9090/tcp  Backup Service
```

---

## 8. File di automazione

Il progetto include un Makefile con target per:

```text
- tidy
- fmt
- vet
- test
- build
- proto
- buf
- docker-build
- docker-up
- docker-down
- docker-clean
- ci
```

Visualizza i target disponibili:

```bash
make help
```
