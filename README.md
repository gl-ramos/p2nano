# p2nano

> Transferência direta entre dispositivos. Sem upload. Sem nuvem.

O **p2nano** é uma ferramenta minimalista para transferir arquivos diretamente
entre dois navegadores usando WebRTC. Não exige conta, banco de dados ou pasta
compartilhada: o servidor faz apenas a sinalização necessária para os
navegadores se encontrarem, e o arquivo é enviado por um `RTCDataChannel` entre
os próprios dispositivos.

A interface está disponível em [p2nano.grlab.me](https://p2nano.grlab.me).
Também é possível executar uma instância própria seguindo as instruções abaixo.

> Projeto em desenvolvimento. O p2nano é um laboratório de WebRTC e uma
> ferramenta para transferências pontuais, não um substituto para uma
> plataforma profissional de compartilhamento de arquivos.

## Funcionalidades

- Transferência ponto a ponto entre navegadores.
- Sem upload, armazenamento permanente ou cópia do arquivo no servidor.
- Código temporário formado por três palavras da lista BIP39 em português.
- Link compartilhável e QR Code para preencher o código automaticamente.
- Arrastar e soltar ou seleção de arquivos.
- Transferência em chunks de **64 KiB**.
- Backpressure com limite de buffer de **1 MiB** no remetente.
- Barra de progresso, velocidade e estimativa de tempo restante.
- Download automático no receptor, com botão de download como alternativa.
- Frontend sem framework, embutido no binário Go.

## Como funciona

1. O remetente seleciona um arquivo.
2. O navegador gera um código de três palavras e cria uma sala temporária no
   servidor.
3. O remetente compartilha as palavras, o link ou o QR Code.
4. O receptor informa o código — ou abre o link — e entra na sala.
5. O servidor retransmite somente as mensagens de sinalização WebRTC: `offer`,
   `answer` e candidatos ICE.
6. Os navegadores negociam uma `RTCPeerConnection` e abrem um
   `RTCDataChannel` chamado `file`.
7. O arquivo é dividido em chunks e enviado diretamente entre os dispositivos.

```text
Remetente                    Servidor                 Receptor
    │                           │                         │
    │── cria sala ────────────>│                         │
    │                           │<──── entra na sala ────│
    │── offer ─────────────────>│──── offer ────────────>│
    │<─ answer ────────────────│<─── answer ─────────────│
    │<──────────── candidatos ICE via sinalização ──────>│
    │                           │                         │
    │<════════════ conexão WebRTC direta ═══════════════>│
    │<════════════ arquivo em chunks ═══════════════════>│
```

O servidor sabe que uma sala existe e participa do *handshake*, mas não recebe
o conteúdo do arquivo, não oferece um endpoint de upload e não grava o arquivo
em disco. Depois da negociação, o WebSocket continua apenas como canal de
sinalização; os bytes seguem pelo WebRTC.

Cada sala comporta um remetente e um receptor. Salas que ainda não encontraram
seu par são removidas após dez minutos. Salas pareadas são encerradas quando os
peers deixam a conexão.

## Código da sala

Para evitar identificadores longos e difíceis de digitar no celular, o p2nano
gera três palavras aleatórias a partir da lista portuguesa do **BIP39**. Um
código pode ter este formato:

```text
janela-tucano-azul
```

A geração ocorre no navegador com `crypto.getRandomValues`. O código não é uma
senha criptográfica nem uma identidade permanente: ele funciona como o
endereço temporário da sala. Qualquer pessoa que possua as três palavras pode
tentar entrar nela enquanto estiver disponível.

O remetente pode copiar o código, copiar um link ou exibir um QR Code. Ao abrir
o link no receptor, as palavras são preenchidas automaticamente e basta tocar
em **Conectar**.

## Transferência de arquivos

A primeira mensagem enviada pelo canal contém apenas os metadados do arquivo:

```json
{
  "name": "relatorio.pdf",
  "size": 4821930,
  "type": "application/pdf"
}
```

Depois, o navegador lê o conteúdo em pedaços de 64 KiB, converte cada pedaço em
`ArrayBuffer` e o envia pelo WebRTC. O receptor acumula os chunks até receber a
mensagem de controle `{ "done": true }`; então reconstrói um `Blob` e inicia o
download.

O remetente usa `bufferedAmount` e configura
`bufferedAmountLowThreshold` para 1 MiB. Quando o buffer de saída passa desse
limite, o envio aguarda o evento `bufferedamountlow` antes de continuar. Esse
mecanismo de *backpressure* evita que a produção de dados cresça
indefinidamente quando a rede não consegue transmitir na mesma velocidade.

## Requisitos

### Execução local

- Go 1.22 ou superior
- Navegador moderno com suporte a WebRTC

### Docker

- Docker com Docker Compose
- A imagem atualmente compila o binário para `linux/arm64`, conforme o
  `Dockerfile`. Em máquinas `x86_64`, use uma plataforma compatível ou ajuste o
  `GOARCH` do Dockerfile para `amd64`.

## Executando localmente

Na raiz do projeto:

```bash
go run .
```

O servidor escuta na porta `8080` por padrão. Acesse:

```text
http://localhost:8080
```

Para usar outra porta:

```bash
PORT=3000 go run .
```

Para gerar e executar um binário:

```bash
go build -o p2nano .
./p2nano
```

O frontend é incorporado ao binário por meio de `go:embed`; por isso, a pasta
`frontend/` precisa estar presente durante a compilação.

## Executando com Docker Compose

```bash
docker compose up --build
```

Depois, abra `http://localhost:8080`. Para executar em segundo plano:

```bash
docker compose up --build -d
```

A aplicação também pode ser colocada atrás de um proxy reverso com HTTPS e
encaminhamento de WebSocket. Quando acessado por HTTPS, o frontend usa
automaticamente `wss://` para a sinalização.

## Configuração

O servidor expõe:

- `GET /`: frontend estático embutido.
- `GET /ws`: endpoint WebSocket de sinalização.

A aplicação não possui banco de dados nem arquivos de configuração adicionais.
A variável de ambiente disponível é:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `PORT` | `8080` | Porta HTTP/WebSocket do servidor |

A negociação ICE utiliza os servidores STUN públicos do Google. Não há servidor
TURN configurado; em redes com NAT, firewall corporativo ou outras restrições,
a conexão direta pode não ser possível e a transferência pode falhar.

## Arquitetura

```text
frontend/                 Interface e lógica do navegador
  index.html              Estrutura da aplicação
  style.css               Estilos
  app.js                  Sinalização, WebRTC e transferência
  bip39_pt.js             Palavras usadas nos códigos de sala
  qrcode.min.js            Biblioteca de QR Code
main.go                   HTTP, WebSocket e hub de salas
Dockerfile                Build multi-stage e imagem Alpine
docker-compose.yml         Execução por Docker Compose
go.mod / go.sum           Dependências Go
```

O backend é um servidor HTTP e WebSocket escrito em Go, usando
[`gorilla/websocket`](https://github.com/gorilla/websocket) para a sinalização.
O hub de salas fica em memória, protegido por mutex, e mantém no máximo um
`sender` e um `receiver` por sala.

O frontend é composto por HTML, CSS e JavaScript sem framework. Não existe
`node_modules`, bundler ou runtime JavaScript no servidor. Durante a compilação,
o `go:embed` incorpora a interface, a lista BIP39 e a biblioteca de QR Code em
um único binário autocontido.

O frontend também mantém candidatos ICE recebidos antes do
`setRemoteDescription` em uma fila temporária, garantindo que mensagens que
chegam fora da ordem esperada não interrompam a negociação.

## Privacidade e limitações

- O arquivo não é persistido pelo servidor e não passa pelo handler do
  WebSocket como conteúdo da transferência.
- O servidor consegue observar o código da sala e as mensagens de sinalização
  necessárias para estabelecer a conexão.
- O WebRTC protege o transporte com a criptografia do próprio protocolo, mas o
  código de três palavras não autentica formalmente a identidade do outro
  participante.
- Qualquer pessoa que descubra o código pode tentar entrar na sala enquanto ela
  estiver disponível.
- O estado das salas existe somente na memória. Reiniciar o processo encerra
  as salas ativas.
- Não há retomada de transferência, checksum por bloco ou fila persistente. Se
  a conexão cair, é necessário iniciar a transferência novamente.
- O receptor acumula os chunks na memória do navegador até conseguir criar o
  `Blob`; arquivos muito grandes podem consumir bastante memória.
- A transferência depende de ambos os navegadores permanecerem conectados e de
  a negociação WebRTC conseguir atravessar a rede.

A ausência de upload elimina a persistência do arquivo em um serviço de
terceiros, mas não representa segurança absoluta. Para dados médicos,
financeiros ou corporativos, ainda seria necessário avaliar autenticação,
confirmação de identidade, integridade e uma política de segurança adequada.

## Desenvolvimento

Após alterar o código, execute:

```bash
gofmt -w main.go
go test ./...
go build ./...
```

Atualmente não há testes automatizados no repositório, mas `go test ./...`
pode ser usado para validar a compilação dos pacotes.

## Licença

Este projeto está licenciado sob a [MIT License](https://opensource.org/licenses/MIT).

```text
MIT License

Copyright (c) 2026 Guilherme Ramos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
