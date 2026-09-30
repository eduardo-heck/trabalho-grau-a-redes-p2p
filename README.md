# Sistema P2P de Sincronização de Arquivos via UDP

Projeto desenvolvido para a disciplina de **Redes de Computadores: Aplicação e Transporte** da UNISINOS.

O sistema implementa uma rede **P2P (Peer-to-Peer)** para sincronização de arquivos entre diferentes computadores utilizando o protocolo **UDP**.

Cada peer atua simultaneamente como cliente e servidor, podendo enviar e receber arquivos dos demais peers da rede.

---

## Objetivo

O objetivo do projeto é manter um diretório `tmp` sincronizado entre todos os peers da rede.

Quando um arquivo é:

- adicionado;
- modificado;
- removido;

em um dos peers, a alteração é propagada para os demais peers.

A comunicação é realizada através de **UDP**, utilizando uma lista estática de peers definida em um arquivo de configuração.

---

## Arquitetura

Cada máquina executa uma instância do programa `peer.py`.

Cada peer possui:

- um identificador;
- um endereço IP;
- uma porta UDP;
- uma lista dos demais peers conhecidos;
- um diretório `tmp` para os arquivos sincronizados.

Exemplo:

```text
             ┌─────────────┐
             │   Peer 1    │
             │ UDP :5001   │
             └──────┬──────┘
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
   ┌─────────────┐     ┌─────────────┐
   │   Peer 2    │◄───►│   Peer 3    │
   │ UDP :5002   │     │ UDP :5003   │
   └─────────────┘     └─────────────┘
```

A rede é estática: cada peer conhece previamente os endereços dos outros peers através do arquivo de configuração.

---

## Tecnologias utilizadas

- **Python 3**
- **UDP**
- `socket`
- `threading`
- `json`
- `hashlib`
- `base64`
- Git / GitHub

---

## Estrutura do projeto

```text
TrabalhoGrauA/
│
├── tmp/
│   └── .gitkeep
│
├── .gitignore
├── config_exemplo.json
├── peer.py
└── README.md
```

### `peer.py`

Implementação principal do peer, responsável por:

- iniciar o servidor UDP;
- enviar e receber mensagens;
- monitorar o diretório `tmp`;
- anunciar alterações;
- solicitar arquivos;
- transferir arquivos em partes;
- confirmar o recebimento das partes;
- detectar arquivos removidos;
- sincronizar novos peers.

### `config_exemplo.json`

Exemplo de configuração de um peer.

Cada máquina deve possuir seu próprio arquivo `config.json`, contendo seu identificador, porta e lista dos demais peers.

### `tmp/`

Diretório utilizado para armazenar os arquivos sincronizados pela rede.

---

## Comunicação

A comunicação entre os peers utiliza o protocolo **UDP**.

Foram definidos diferentes tipos de mensagens para controlar a sincronização:

### ANNOUNCE

Informa aos demais peers que um arquivo foi criado ou modificado.

A mensagem contém informações sobre o arquivo, como:

- nome;
- tamanho;
- hash.

---

### REQUEST

Solicita a transferência de um determinado arquivo a outro peer.

---

### DATA

Transporta uma parte do arquivo.

Como o UDP trabalha com datagramas e não garante a entrega dos dados, arquivos são divididos em partes menores para a transferência.

---

### ACK

Confirma o recebimento de uma parte do arquivo.

Caso uma confirmação não seja recebida, a parte pode ser retransmitida.

---

### REMOVE

Informa aos demais peers que um arquivo foi removido.

A operação possui um identificador para evitar que a mesma remoção seja propagada indefinidamente pela rede.

---

### LIST_REQUEST / LIST_RESPONSE

Utilizadas para sincronizar um novo peer que entra na rede.

O novo peer solicita a lista de arquivos existentes e, posteriormente, solicita os arquivos que ainda não possui.

---

## Transferência de arquivos

Os arquivos são transferidos em partes.

O processo funciona aproximadamente da seguinte forma:

```text
Peer A                         Peer B
  │                              │
  │──── ANNOUNCE ───────────────►│
  │                              │
  │◄──── REQUEST ────────────────│
  │                              │
  │──── DATA (parte 1) ─────────►│
  │◄──── ACK ────────────────────│
  │                              │
  │──── DATA (parte 2) ─────────►│
  │◄──── ACK ────────────────────│
  │                              │
  │          ...                 │
  │                              │
  │──── DATA (parte N) ─────────►│
  │◄──── ACK ────────────────────│
  │                              │
  │       Arquivo completo       │
```

O receptor consegue lidar com partes recebidas fora de ordem e utiliza as informações recebidas para reconstruir o arquivo.

Após a transferência, o tamanho e o hash do arquivo são verificados.

---

## Monitoramento do diretório

O programa possui uma thread responsável por monitorar continuamente o diretório `tmp`.

Quando uma alteração é detectada, o peer comunica os demais peers.

### Arquivo criado

```text
Peer 1:
tmp/
└── arquivo.txt
```

O Peer 1 anuncia o arquivo e os outros peers solicitam seu conteúdo.

Resultado:

```text
Peer 1       Peer 2       Peer 3
arquivo.txt  arquivo.txt  arquivo.txt
```

### Arquivo modificado

Quando o conteúdo de um arquivo é alterado, a mudança é detectada e o arquivo é novamente sincronizado.

### Arquivo removido

Quando um arquivo é removido de um peer, a operação de remoção é propagada para os demais peers.

---

## Entrada de um novo peer

Um novo peer pode entrar na rede mesmo iniciando com o diretório `tmp` vazio.

O processo é:

```text
Novo Peer
   │
   │ LIST_REQUEST
   ▼
Peers existentes
   │
   │ LIST_RESPONSE
   ▼
Lista de arquivos
   │
   │ REQUEST
   ▼
Transferência dos arquivos
```

Dessa forma, o novo peer consegue obter os arquivos que já existem na rede.

---

## Configuração

Cada peer possui um arquivo `config.json`.

Exemplo:

```json
{
    "peer_id": "peer1",
    "host": "0.0.0.0",
    "port": 5001,
    "peers": [
        {
            "id": "peer2",
            "host": "IP_DO_PEER_2",
            "port": 5002
        },
        {
            "id": "peer3",
            "host": "IP_DO_PEER_3",
            "port": 5003
        }
    ]
}
```

O arquivo `config.json` é específico de cada máquina e não deve ser enviado ao repositório.

Por isso, ele está incluído no `.gitignore`.

O arquivo `config_exemplo.json` serve como modelo para criar a configuração de cada peer.

---

## Como executar

### 1. Instalar Python

É necessário possuir o Python 3 instalado.

Verifique com:

```bash
python --version
```

---

### 2. Configurar o peer

Crie um arquivo chamado:

```text
config.json
```

baseado no:

```text
config_exemplo.json
```

Configure o identificador, porta e os demais peers da rede.

---

### 3. Executar

Na pasta do projeto:

```bash
python peer.py
```

Também é possível informar outro arquivo de configuração:

```bash
python peer.py config.json
```

---

## Testes realizados

O sistema foi testado com três peers executando em computadores físicos diferentes e conectados através de uma rede virtual.

Foram realizados testes de:

- comunicação entre peers;
- criação de arquivos;
- sincronização automática;
- modificação de arquivos;
- remoção de arquivos;
- transferência de arquivos;
- transferência em partes;
- confirmação de recebimento;
- sincronização de um novo peer;
- inicialização de um peer com `tmp` vazio.

Durante os testes, os arquivos foram corretamente propagados entre os três peers.

Também foi testada a entrada de um quarto peer com o diretório `tmp` inicialmente vazio, que recebeu os arquivos existentes na rede.

---

## Tratamento das limitações do UDP

O UDP não garante:

- entrega dos datagramas;
- ordem de chegada;
- ausência de duplicação.

Para reduzir esses problemas na aplicação, o sistema utiliza mecanismos próprios, como:

- divisão dos arquivos em partes;
- confirmação de recebimento através de ACK;
- retransmissão de partes quando necessário;
- identificação das partes recebidas;
- reconstrução do arquivo após o recebimento;
- verificação do arquivo através de hash;
- identificadores para evitar propagação repetida de operações.

---

## Autores

**Eduardo Müller Heck**

Universidade do Vale do Rio dos Sinos — UNISINOS

Disciplina: Redes de Computadores: Aplicação e Transporte

2026