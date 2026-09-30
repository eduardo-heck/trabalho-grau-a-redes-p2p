import socket
import json
import threading
import sys
import os
import time
import base64
import hashlib
import uuid


# ==========================================
# CONFIGURAÇÃO
# ==========================================

nome_config = sys.argv[1] if len(sys.argv) > 1 else "config.json"

with open(nome_config, "r", encoding="utf-8") as arquivo:
    config = json.load(arquivo)

PEER_ID = config["peer_id"]
HOST = config["host"]
PORT = config["port"]
PEERS = config["peers"]

DIRETORIO_TMP = "tmp"

# Tamanho pequeno para evitar fragmentação UDP
TAMANHO_CHUNK = 700

# Quantidade máxima de tentativas para cada chunk
MAX_TENTATIVAS = 5

# Tempo esperando ACK
TIMEOUT_ACK = 1.0


# ==========================================
# SOCKET UDP
# ==========================================

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))


# ==========================================
# CONTROLE COMPARTILHADO
# ==========================================

lock = threading.Lock()

# Arquivos conhecidos pelo monitor.
# Formato:
# {
#     "arquivo.txt": (tamanho, hash)
# }
arquivos_conhecidos = {}

# Eventos usados para esperar ACKs.
# Chave:
# (IP, porta, nome_arquivo, numero_chunk)
eventos_ack = {}

# Operações REMOVE já processadas
operacoes_remocao = set()

# Arquivos que estão sendo baixados
downloads_em_andamento = set()

# Guarda a última lista de arquivos recebida de cada peer
listas_peers = {}


# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def listar_arquivos():

    if not os.path.exists(DIRETORIO_TMP):
        os.makedirs(DIRETORIO_TMP)

    arquivos = set()

    for nome in os.listdir(DIRETORIO_TMP):

        caminho = os.path.join(
            DIRETORIO_TMP,
            nome
        )

        if os.path.isfile(caminho):
            arquivos.add(nome)

    return arquivos


def caminho_seguro(nome):

    # Impede que um nome recebido pela rede
    # tente acessar diretórios fora do tmp.
    nome = os.path.basename(nome)

    return os.path.join(
        DIRETORIO_TMP,
        nome
    )


def calcular_hash(caminho):

    sha256 = hashlib.sha256()

    with open(caminho, "rb") as arquivo:

        while True:

            bloco = arquivo.read(4096)

            if not bloco:
                break

            sha256.update(bloco)

    return sha256.hexdigest()


def obter_metadados(nome):

    caminho = caminho_seguro(nome)

    if not os.path.exists(caminho):
        return None

    tamanho = os.path.getsize(caminho)
    hash_arquivo = calcular_hash(caminho)

    return {
        "name": nome,
        "size": tamanho,
        "hash": hash_arquivo
    }


def encontrar_peer(ip, porta):

    for peer in PEERS:

        if peer["host"] == ip and peer["port"] == porta:
            return peer

    return None


def encontrar_peer_por_id(peer_id):

    for peer in PEERS:

        if peer["id"] == peer_id:
            return peer

    return None


def enviar_json(objeto, endereco):

    mensagem = json.dumps(
        objeto,
        ensure_ascii=False
    ).encode("utf-8")

    sock.sendto(
        mensagem,
        endereco
    )


def enviar_para_todos(objeto, ignorar_endereco=None):

    for peer in PEERS:

        endereco = (
            peer["host"],
            peer["port"]
        )

        if endereco == ignorar_endereco:
            continue

        enviar_json(
            objeto,
            endereco
        )

        print(
            f"[ENVIADO] {peer['id']} "
            f"({peer['host']}:{peer['port']}): "
            f"{objeto}"
        )


# ==========================================
# ENVIAR ARQUIVO EM PARTES
# ==========================================

def enviar_arquivo(nome_arquivo, endereco):

    caminho = caminho_seguro(nome_arquivo)

    if not os.path.exists(caminho):

        print(
            f"[ERRO] Arquivo não encontrado: "
            f"{nome_arquivo}"
        )

        return

    with open(caminho, "rb") as arquivo:
        dados = arquivo.read()

    tamanho_total = len(dados)

    if tamanho_total == 0:
        total_chunks = 1
    else:
        total_chunks = (
            (tamanho_total + TAMANHO_CHUNK - 1)
            // TAMANHO_CHUNK
        )

    print()
    print(
        f"[ENVIO] {nome_arquivo}: "
        f"{tamanho_total} bytes, "
        f"{total_chunks} partes"
    )

    for numero_chunk in range(total_chunks):

        inicio = (
            numero_chunk * TAMANHO_CHUNK
        )

        fim = (
            inicio + TAMANHO_CHUNK
        )

        chunk = dados[inicio:fim]

        # Arquivo vazio
        if tamanho_total == 0:
            chunk = b""

        dados_base64 = base64.b64encode(
            chunk
        ).decode("ascii")

        mensagem = {
            "type": "DATA",
            "name": nome_arquivo,
            "seq": numero_chunk,
            "total": total_chunks,
            "size": tamanho_total,
            "data": dados_base64
        }

        chave = (
            endereco[0],
            endereco[1],
            nome_arquivo,
            numero_chunk
        )

        evento = threading.Event()

        with lock:
            eventos_ack[chave] = evento

        recebido = False

        for tentativa in range(1, MAX_TENTATIVAS + 1):

            enviar_json(
                mensagem,
                endereco
            )

            print(
                f"[DATA] {nome_arquivo} "
                f"parte {numero_chunk + 1}/"
                f"{total_chunks} "
                f"(tentativa {tentativa})"
            )

            if evento.wait(TIMEOUT_ACK):

                recebido = True
                break

        with lock:

            eventos_ack.pop(
                chave,
                None
            )

        if not recebido:

            print(
                f"[ERRO] Não foi recebido ACK "
                f"da parte {numero_chunk} "
                f"de {nome_arquivo}"
            )

            return

    print(
        f"[ENVIO CONCLUÍDO] "
        f"{nome_arquivo}"
    )


# ==========================================
# RECEBER ARQUIVO
# ==========================================

def processar_data(mensagem, endereco):

    nome = mensagem.get("name")
    seq = mensagem.get("seq")
    total = mensagem.get("total")
    tamanho = mensagem.get("size")
    dados_base64 = mensagem.get("data")

    if (
        nome is None
        or seq is None
        or total is None
        or dados_base64 is None
    ):
        return

    nome = os.path.basename(nome)

    try:

        seq = int(seq)
        total = int(total)
        tamanho = int(tamanho)

        dados = base64.b64decode(
            dados_base64
        )

    except Exception as erro:

        print(
            f"[ERRO DATA] {erro}"
        )

        return

    # Envia ACK imediatamente.
    # Isso permite ao remetente continuar.
    ack = {
        "type": "ACK",
        "name": nome,
        "seq": seq
    }

    enviar_json(
        ack,
        endereco
    )

    # Guardamos as partes em memória.
    # Para os arquivos pequenos da demonstração,
    # isso é suficiente.
    if not hasattr(processar_data, "downloads"):
        processar_data.downloads = {}

    chave = (
        endereco,
        nome
    )

    if chave not in processar_data.downloads:

        processar_data.downloads[chave] = {
            "total": total,
            "size": tamanho,
            "chunks": {}
        }

    download = processar_data.downloads[chave]

    # Ignora duplicata sem adicionar duas vezes
    download["chunks"][seq] = dados

    recebidas = len(
        download["chunks"]
    )

    print(
        f"[DATA RECEBIDO] {nome} "
        f"parte {seq + 1}/{total}"
    )

    # Ainda faltam partes
    if recebidas < total:
        return

    print(
        f"[MONTANDO] {nome}"
    )

    # Verifica se todas as partes existem
    for numero in range(total):

        if numero not in download["chunks"]:

            print(
                f"[ERRO] Falta a parte "
                f"{numero} de {nome}"
            )

            with lock:
                downloads_em_andamento.discard(nome)

            return

    caminho = caminho_seguro(nome)

    arquivo_temporario = (
        caminho + ".part"
    )

    try:

        with open(
            arquivo_temporario,
            "wb"
        ) as arquivo:

            for numero in range(total):

                arquivo.write(
                    download["chunks"][numero]
                )

        tamanho_real = os.path.getsize(
            arquivo_temporario
        )

        if tamanho_real != tamanho:

            print(
                f"[ERRO] Tamanho incorreto "
                f"para {nome}: "
                f"{tamanho_real}/{tamanho}"
            )

            os.remove(
                arquivo_temporario
            )

            with lock:
                downloads_em_andamento.discard(nome)

            return

        os.replace(
            arquivo_temporario,
            caminho
        )

        print(
            f"[ARQUIVO RECEBIDO] "
            f"{nome} "
            f"({tamanho_real} bytes)"
        )

        # Atualiza o estado do monitor para que
        # ele NÃO anuncie novamente o arquivo
        # que acabou de chegar pela rede.
        metadados = obter_metadados(nome)

        with lock:

            if metadados:

                arquivos_conhecidos[nome] = (
                    metadados["size"],
                    metadados["hash"]
                )

        del processar_data.downloads[chave]

        with lock:
            downloads_em_andamento.discard(nome)

    except Exception as erro:

        print(
            f"[ERRO AO SALVAR] {erro}"
        )


# ==========================================
# PROCESSAR MENSAGENS
# ==========================================

def processar_mensagem(mensagem, endereco):

    tipo = mensagem.get("type")


    # --------------------------------------
    # ANNOUNCE
    # --------------------------------------

    if tipo == "ANNOUNCE":

        nome = os.path.basename(
            mensagem.get("name", "")
        )

        tamanho = int(
            mensagem.get("size", 0)
        )

        hash_remoto = mensagem.get(
            "hash",
            ""
        )

        print(
            f"[ANNOUNCE] "
            f"{nome} ({tamanho} bytes)"
        )

        caminho = caminho_seguro(nome)

        precisa_baixar = False

        if not os.path.exists(caminho):

            precisa_baixar = True

        else:

            tamanho_local = os.path.getsize(
                caminho
            )

            if tamanho_local != tamanho:

                precisa_baixar = True

            elif hash_remoto:

                hash_local = calcular_hash(
                    caminho
                )

                if hash_local != hash_remoto:

                    precisa_baixar = True

        if not precisa_baixar:

            print(
                f"[ANNOUNCE] "
                f"{nome} já está atualizado."
            )

            return

        peer_origem = encontrar_peer(
            endereco[0],
            endereco[1]
        )

        if peer_origem is None:

            print(
                "[ERRO] Peer de origem "
                "não encontrado."
            )

            return

        with lock:

            if nome in downloads_em_andamento:

                return

            downloads_em_andamento.add(
                nome
            )

        mensagem_request = {
            "type": "REQUEST",
            "name": nome
        }

        enviar_json(
            mensagem_request,
            (
                peer_origem["host"],
                peer_origem["port"]
            )
        )

        print(
            f"[REQUEST] Solicitando "
            f"{nome} para "
            f"{peer_origem['id']}"
        )


    # --------------------------------------
    # REQUEST
    # --------------------------------------

    elif tipo == "REQUEST":

        nome = os.path.basename(
            mensagem.get("name", "")
        )

        print(
            f"[REQUEST] "
            f"Recebido pedido por {nome}"
        )

        # O envio ocorre em outra thread
        # para o servidor continuar recebendo
        # mensagens e ACKs.
        thread = threading.Thread(
            target=enviar_arquivo,
            args=(nome, endereco),
            daemon=True
        )

        thread.start()


    # --------------------------------------
    # DATA
    # --------------------------------------

    elif tipo == "DATA":

        processar_data(
            mensagem,
            endereco
        )


    # --------------------------------------
    # ACK
    # --------------------------------------

    elif tipo == "ACK":

        nome = os.path.basename(
            mensagem.get("name", "")
        )

        seq = int(
            mensagem.get("seq", -1)
        )

        chave = (
            endereco[0],
            endereco[1],
            nome,
            seq
        )

        with lock:

            evento = eventos_ack.get(
                chave
            )

        if evento:

            evento.set()

            print(
                f"[ACK] {nome} "
                f"parte {seq + 1}"
            )


    # --------------------------------------
    # REMOVE
    # --------------------------------------

    elif tipo == "REMOVE":

        nome = os.path.basename(
            mensagem.get("name", "")
        )

        operacao = mensagem.get(
            "operation_id"
        )

        if not operacao:

            operacao = str(
                uuid.uuid4()
            )

        with lock:

            if operacao in operacoes_remocao:

                return

            operacoes_remocao.add(
                operacao
            )

        caminho = caminho_seguro(nome)

        if os.path.exists(caminho):

            try:

                os.remove(caminho)

                print(
                    f"[REMOVIDO] {nome}"
                )

            except Exception as erro:

                print(
                    f"[ERRO REMOVE] {erro}"
                )

        # Atualiza o monitor para impedir
        # que ele gere outro REMOVE.
        with lock:

            arquivos_conhecidos.pop(
                nome,
                None
            )

        # Propaga para os demais peers
        enviar_para_todos(
            mensagem,
            ignorar_endereco=endereco
        )


    # --------------------------------------
    # LIST_REQUEST
    # --------------------------------------

    elif tipo == "LIST_REQUEST":

        arquivos = []

        for nome in listar_arquivos():

            metadados = obter_metadados(
                nome
            )

            if metadados:

                arquivos.append(
                    metadados
                )

        resposta = {
            "type": "LIST_RESPONSE",
            "files": arquivos
        }

        enviar_json(
            resposta,
            endereco
        )

        print(
            f"[LISTA] Enviada para "
            f"{endereco}"
        )


    # --------------------------------------
    # LIST_RESPONSE
    # --------------------------------------

    elif tipo == "LIST_RESPONSE":

        arquivos = mensagem.get(
            "files",
            []
        )

        print(
            f"[LISTA] Recebidos "
            f"{len(arquivos)} arquivos "
            f"de {endereco}"
        )

        # Guarda a lista recebida para exibição
        peer_info = encontrar_peer(
            endereco[0],
            endereco[1]
        )

        if peer_info:

            listas_peers[peer_info["id"]] = {
                "host": endereco[0],
                "port": endereco[1],
                "files": arquivos
            }

        for arquivo in arquivos:

            nome = os.path.basename(
                arquivo.get("name", "")
            )

            tamanho = int(
                arquivo.get("size", 0)
            )

            hash_remoto = arquivo.get(
                "hash",
                ""
            )

            caminho = caminho_seguro(nome)

            precisa_baixar = False

            if not os.path.exists(caminho):

                precisa_baixar = True

            else:

                tamanho_local = os.path.getsize(
                    caminho
                )

                if tamanho_local != tamanho:

                    precisa_baixar = True

                elif hash_remoto:

                    hash_local = calcular_hash(
                        caminho
                    )

                    if hash_local != hash_remoto:

                        precisa_baixar = True

            if precisa_baixar:

                with lock:

                    if nome in downloads_em_andamento:

                        continue

                    downloads_em_andamento.add(
                        nome
                    )

                request = {
                    "type": "REQUEST",
                    "name": nome
                }

                enviar_json(
                    request,
                    endereco
                )

                print(
                    f"[REQUEST] "
                    f"{nome} via LIST"
                )


# ==========================================
# RECEBER MENSAGENS
# ==========================================

def receber_mensagens():

    while True:

        try:

            mensagem_bytes, endereco = (
                sock.recvfrom(65535)
            )

            try:

                mensagem_texto = (
                    mensagem_bytes.decode(
                        "utf-8"
                    )
                )

                mensagem = json.loads(
                    mensagem_texto
                )

            except (
                UnicodeDecodeError,
                json.JSONDecodeError
            ):

                print(
                    f"[ERRO] Pacote inválido "
                    f"recebido de {endereco}"
                )

                continue

            print()

            print(
                f"[RECEBIDO] {endereco}: "
                f"{mensagem}"
            )

            processar_mensagem(
                mensagem,
                endereco
            )

            print()

        except Exception as erro:

            print(
                f"[ERRO RECEPÇÃO] {erro}"
            )


# ==========================================
# MONITORAR TMP
# ==========================================

def inicializar_estado_arquivos():

    with lock:

        arquivos_conhecidos.clear()

        for nome in listar_arquivos():

            metadados = obter_metadados(
                nome
            )

            if metadados:

                arquivos_conhecidos[nome] = (
                    metadados["size"],
                    metadados["hash"]
                )


def monitorar_tmp():

    inicializar_estado_arquivos()

    while True:

        time.sleep(1)

        arquivos_atuais = listar_arquivos()

        estado_atual = {}

        for nome in arquivos_atuais:

            metadados = obter_metadados(
                nome
            )

            if metadados:

                estado_atual[nome] = (
                    metadados["size"],
                    metadados["hash"]
                )

        # ----------------------------------
        # NOVOS OU MODIFICADOS
        # ----------------------------------

        for nome, estado in estado_atual.items():

            enviar_announce = False

            with lock:

                estado_anterior = (
                    arquivos_conhecidos.get(nome)
                )

            if estado_anterior is None:

                print()

                print(
                    f"[NOVO ARQUIVO] "
                    f"{nome} "
                    f"({estado[0]} bytes)"
                )

                enviar_announce = True

            elif estado_anterior != estado:

                print()

                print(
                    f"[ARQUIVO MODIFICADO] "
                    f"{nome} "
                    f"({estado[0]} bytes)"
                )

                enviar_announce = True

            if enviar_announce:

                mensagem = {
                    "type": "ANNOUNCE",
                    "name": nome,
                    "size": estado[0],
                    "hash": estado[1]
                }

                enviar_para_todos(
                    mensagem
                )

            with lock:

                arquivos_conhecidos[nome] = estado


        # ----------------------------------
        # ARQUIVOS REMOVIDOS
        # ----------------------------------

        with lock:

            nomes_conhecidos = set(
                arquivos_conhecidos.keys()
            )

        removidos = (
            nomes_conhecidos -
            set(estado_atual.keys())
        )

        for nome in removidos:

            with lock:

                # Se outro processo já removeu
                # e atualizou o estado, não envia.
                if nome not in arquivos_conhecidos:
                    continue

                arquivos_conhecidos.pop(
                    nome,
                    None
                )

            print()

            print(
                f"[ARQUIVO REMOVIDO] "
                f"{nome}"
            )

            mensagem = {
                "type": "REMOVE",
                "name": nome,
                "operation_id": str(
                    uuid.uuid4()
                )
            }

            enviar_para_todos(
                mensagem
            )


# ==========================================
# MENSAGEM MANUAL
# ==========================================
def mostrar_informacoes_rede():

    print()
    print("=" * 50)
    print("           INFORMAÇÕES DA REDE")
    print("=" * 50)

    # --------------------------------------
    # PEER LOCAL
    # --------------------------------------

    arquivos_locais = listar_arquivos()

    print()
    print(f"Peer local: {PEER_ID}")
    print(f"Endereço: {HOST}:{PORT}")
    print(
        f"Arquivos locais: "
        f"{len(arquivos_locais)}"
    )

    for nome in sorted(arquivos_locais):

        caminho = caminho_seguro(nome)
        tamanho = os.path.getsize(caminho)

        print(
            f"  - {nome} "
            f"({tamanho} bytes)"
        )

    # --------------------------------------
    # OUTROS PEERS
    # --------------------------------------

    print()
    print(
        f"Peers configurados: "
        f"{len(PEERS)}"
    )

    for peer in PEERS:

        peer_id = peer["id"]

        print()
        print(
            f"[{peer_id}] "
            f"{peer['host']}:{peer['port']}"
        )

        if peer_id in listas_peers:

            arquivos = listas_peers[
                peer_id
            ]["files"]

            print("  Status: ATIVO")
            print(
                f"  Arquivos: "
                f"{len(arquivos)}"
            )

            for arquivo in sorted(
                arquivos,
                key=lambda x: x.get("name", "")
            ):

                nome = os.path.basename(
                    arquivo.get("name", "")
                )

                tamanho = int(
                    arquivo.get("size", 0)
                )

                print(
                    f"    - {nome} "
                    f"({tamanho} bytes)"
                )

        else:

            print("  Status: SEM INFORMAÇÃO")
            print("  Arquivos: -")

    print()
    print("=" * 50)
    print()


def enviar_mensagem():

    print()
    print("Peers disponíveis:")

    for i, peer in enumerate(
        PEERS,
        start=1
    ):

        print(
            f"{i} - {peer['id']} "
            f"({peer['host']}:{peer['port']})"
        )

    escolha = input(
        "Escolha o peer: "
    )

    try:

        indice = int(escolha) - 1

        peer_destino = PEERS[indice]

    except (
        ValueError,
        IndexError
    ):

        print("Peer inválido.")

        return

    mensagem = input(
        "Digite a mensagem: "
    )

    objeto = {
        "type": "MESSAGE",
        "text": mensagem
    }

    enviar_json(
        objeto,
        (
            peer_destino["host"],
            peer_destino["port"]
        )
    )

    print(
        f"[ENVIADO] Para "
        f"{peer_destino['id']}: "
        f"{mensagem}"
    )


# ==========================================
# SINCRONIZAÇÃO INICIAL
# ==========================================

def solicitar_listas():

    print()
    print(
        "[SYNC] Solicitando lista "
        "dos peers..."
    )

    mensagem = {
        "type": "LIST_REQUEST"
    }

    for peer in PEERS:

        enviar_json(
            mensagem,
            (
                peer["host"],
                peer["port"]
            )
        )

        print(
            f"[SYNC] LIST_REQUEST → "
            f"{peer['id']}"
        )


# ==========================================
# INICIALIZAÇÃO
# ==========================================

thread_receber = threading.Thread(
    target=receber_mensagens,
    daemon=True
)

thread_receber.start()


thread_monitorar = threading.Thread(
    target=monitorar_tmp,
    daemon=True
)

thread_monitorar.start()


print("=" * 50)
print("              SISTEMA P2P - PEER")
print("=" * 50)

print(
    f"Peer: {PEER_ID}"
)

print(
    f"Endereço: {HOST}:{PORT}"
)

print(
    "Servidor UDP iniciado."
)

print(
    "Monitoramento do diretório tmp iniciado."
)

print()


# Pequeno intervalo para garantir que
# o servidor já esteja pronto.
time.sleep(1)

solicitar_listas()

print()

# ==========================================
# INTERFACE
# ==========================================

while True:

    print(
    "1 - Enviar mensagem"
    )

    print(
        "2 - Listar arquivos"
    )

    print(
        "3 - Informações da rede"
    )

    print(
        "4 - Sair"
    )

    opcao = input(
        "Escolha uma opção: "
    )


    if opcao == "1":

        enviar_mensagem()


    elif opcao == "2":

        arquivos = listar_arquivos()

        print()

        print(
            f"[ARQUIVOS LOCAIS] "
            f"{len(arquivos)} arquivo(s)"
        )

        for nome in sorted(arquivos):

            caminho = caminho_seguro(
                nome
            )

            tamanho = os.path.getsize(
                caminho
            )

            print(
                f" - {nome} "
                f"({tamanho} bytes)"
            )

        print()


    elif opcao == "3":

        mostrar_informacoes_rede()


    elif opcao == "4":

        print(
            "Encerrando peer..."
        )

        break


    else:

        print(
            "Opção inválida."
        )


    print()