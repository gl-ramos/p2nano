/* ──────────────────────────────────────────────────────────────────────────
   p2nano — app.js
   Responsável por:
     1. Navegação entre telas / passos
     2. Geração de código de sala (3 palavras BIP39 PT)
     3. Signaling via WebSocket
     4. Conexão P2P via WebRTC (RTCPeerConnection + RTCDataChannel)
     5. Transferência de arquivo em chunks de 64 KB
     6. Download automático no receptor
     7. QR Code gerado via Canvas puro (sem dependências externas)
────────────────────────────────────────────────────────────────────────── */

"use strict";

// ── Constantes ───────────────────────────────────────────────────────────────
const CHUNK_SIZE = 64 * 1024; // 64 KB por chunk
const BUFFER_THRESHOLD = 1 * 1024 * 1024; // pausa envio se buffer > 1 MB
const WS_URL = (() => {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}/ws`;
})();

// ── BIP39 Set para lookups O(1) ──────────────────────────────────────────────
// BIP39_PT is defined in bip39_pt.js (loaded before this script).
const BIP39_SET = new Set(BIP39_PT);

// ── Utilitários ──────────────────────────────────────────────────────────────
function randomRoomCode() {
  const arr = new Uint16Array(3);
  crypto.getRandomValues(arr);
  return [
    BIP39_PT[arr[0] % 2048],
    BIP39_PT[arr[1] % 2048],
    BIP39_PT[arr[2] % 2048],
  ].join("-");
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

function formatSpeed(bps) {
  if (bps < 1024) return bps.toFixed(0) + " B/s";
  if (bps < 1024 * 1024) return (bps / 1024).toFixed(0) + " KB/s";
  if (bps < 1024 * 1024 * 1024) return (bps / (1024 * 1024)).toFixed(2) + " MB/s";
  return (bps / (1024 * 1024 * 1024)).toFixed(2) + " GB/s";
}

function formatETA(seconds) {
  if (!isFinite(seconds) || seconds <= 0) return "";
  if (seconds < 60) return `~${Math.ceil(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.ceil(seconds % 60);
  return `~${m}m ${s}s`;
}

// ── DOM helpers ──────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);

function showScreen(id) {
  document
    .querySelectorAll(".screen")
    .forEach((s) => s.classList.remove("active"));
  $(id).classList.add("active");
}

function showStep(screenId, stepId) {
  document
    .querySelectorAll(`#${screenId} .step`)
    .forEach((s) => s.classList.remove("active"));
  $(stepId).classList.add("active");
}

function setFileInfo(elId, name, size) {
  const el = $(elId);
  el.innerHTML = `
    <span class="file-name" title="${name}">${name}</span>
    <span class="file-size">${formatBytes(size)}</span>
  `;
}

function showError(elId, msg) {
  const el = $(elId);
  el.textContent = msg;
  el.classList.remove("hidden");
}

function hideError(elId) {
  $(elId).classList.add("hidden");
}

// ── QR Code (via qrcode.min.js) ──────────────────────────────────────────────
// Wrapper sobre a lib QRCode.js — produz canvas dentro do container.
function drawQRCode(containerEl, text) {
  containerEl.innerHTML = "";
  try {
    new QRCode(containerEl, {
      text: text,
      width: 160,
      height: 160,
      colorDark: "#000000",
      colorLight: "#ffffff",
      correctLevel: QRCode.CorrectLevel.M,
    });
  } catch (e) {
    console.warn("[qr] erro ao gerar QR Code:", e);
  }
}

// ── WebSocket wrapper ────────────────────────────────────────────────────────
class Signaling {
  constructor(onMessage, onClose) {
    this._ws = new WebSocket(WS_URL);
    this._onMessage = onMessage;
    this._onClose = onClose;
    this._intentionalClose = false;
    this._ws.addEventListener("message", (e) => {
      try {
        this._onMessage(JSON.parse(e.data));
      } catch (err) {
        console.error("[ws] parse error", err);
      }
    });
    this._ws.addEventListener("close", () => {
      if (!this._intentionalClose) this._onClose();
    });
    this._ws.addEventListener("error", (err) =>
      console.error("[ws] error", err),
    );
  }

  send(obj) {
    if (this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  }

  ready() {
    return new Promise((resolve, reject) => {
      if (this._ws.readyState === WebSocket.OPEN) return resolve();
      this._ws.addEventListener("open", resolve, { once: true });
      this._ws.addEventListener("error", () => reject(new Error("Falha ao conectar ao servidor.")), { once: true });
    });
  }

  close() {
    this._intentionalClose = true;
    this._ws.close();
  }
}

// ── RTCPeerConnection factory ────────────────────────────────────────────────
const ICE_SERVERS = {
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" },
  ],
};

function createPeer(onIce) {
  const pc = new RTCPeerConnection(ICE_SERVERS);
  pc.addEventListener("icecandidate", (e) => {
    if (e.candidate) onIce(e.candidate);
  });
  return pc;
}

// ── Estado global mínimo ─────────────────────────────────────────────────────
let _sig = null; // Signaling instance
let _pc = null;  // RTCPeerConnection
let _dc = null;  // RTCDataChannel (lado do sender)
let _file = null; // File selecionado
let _receivedBlob = null; // Blob reconstruído no receptor
let _roomURL = null; // URL completa da sala (para copiar link)
// ICE candidates que chegaram antes do setRemoteDescription estar pronto.
let _pendingCandidates = [];

function resetState() {
  if (_sig) {
    try { _sig.close(); } catch (_) {}
    _sig = null;
  }
  if (_pc) {
    try { _pc.close(); } catch (_) {}
    _pc = null;
  }
  _dc = null;
  _file = null;
  _receivedBlob = null;
  _pendingCandidates = [];
}

// ════════════════════════════════════════════════════════════════════════════
//  SENDER
// ════════════════════════════════════════════════════════════════════════════

async function startSender(file) {
  const roomCode = randomRoomCode();
  _file = file;

  // Atualiza UI
  setFileInfo("sender-file-info", file.name, file.size);
  setFileInfo("sender-file-info-2", file.name, file.size);
  $("room-code-display").textContent = roomCode;
  $("sender-status").textContent = "Aguardando receptor conectar...";
  hideError("sender-ws-error");
  showStep("screen-sender", "sender-step-wait");

  // Gera QR Code com URL para auto-preenchimento no receptor
  const qrContainer = $("qr-container");
  const roomURL = `${location.origin}${location.pathname}?room=${roomCode}`;
  _roomURL = roomURL;
  drawQRCode(qrContainer, roomURL);

  // Abre sinalização
  try {
    _sig = new Signaling(onSenderSignal, () => {
      // WS fechou inesperadamente
      showError("sender-ws-error", "Conexão com o servidor perdida. Tente novamente.");
      $("sender-status").textContent = "";
      $("sender-spinner").style.display = "none";
    });
    await _sig.ready();
  } catch (err) {
    showStep("screen-sender", "sender-step-file");
    alert("Não foi possível conectar ao servidor. Verifique sua conexão.");
    return;
  }

  _sig.send({ type: "join", room: roomCode, role: "sender" });

  function onSenderSignal(msg) {
    switch (msg.type) {
      case "joined":
        console.log("[sender] sala criada:", roomCode);
        break;

      case "ready":
        console.log("[sender] receptor conectou, iniciando WebRTC");
        $("sender-status").textContent =
          "Receptor conectado. Iniciando conexão P2P...";
        initSenderPeer();
        break;

      case "answer":
        _pc.setRemoteDescription(new RTCSessionDescription(msg.payload))
          .then(() => {
            // Flush any ICE candidates that arrived before the remote description.
            const queued = _pendingCandidates.splice(0);
            return Promise.all(
              queued.map((c) => _pc.addIceCandidate(new RTCIceCandidate(c)).catch(console.warn))
            );
          })
          .catch(console.error);
        break;

      case "ice":
        if (_pc && _pc.remoteDescription) {
          _pc.addIceCandidate(new RTCIceCandidate(msg.payload)).catch(console.warn);
        } else {
          _pendingCandidates.push(msg.payload);
        }
        break;

      case "peer_left":
        console.warn("[sender] receptor desconectou");
        // Só mostra erro se a transferência ainda estiver em curso
        const transferStep = document.querySelector("#sender-step-transfer.active");
        if (transferStep) {
          showError("sender-transfer-error", "O receptor desconectou durante a transferência.");
        }
        break;

      case "error":
        console.error("[sender] erro do servidor:", msg.payload);
        break;
    }
  }
}

function initSenderPeer() {
  _pc = createPeer((candidate) => {
    _sig.send({ type: "ice", payload: candidate });
  });

  // Cria DataChannel
  _dc = _pc.createDataChannel("file", { ordered: true });
  _dc.binaryType = "arraybuffer";

  _dc.addEventListener("open", () => {
    console.log("[sender] DataChannel aberto, iniciando envio");
    sendFile(_file, _dc);
  });

  _dc.addEventListener("error", (e) => console.error("[sender] DC error", e));

  _pc
    .createOffer()
    .then((offer) => _pc.setLocalDescription(offer))
    .then(() => _sig.send({ type: "offer", payload: _pc.localDescription }))
    .catch(console.error);
}

// Envia o arquivo em chunks usando o evento bufferedamountlow para
// backpressure — evita busy-polling e reduz latência.
async function sendFile(file, dc) {
  const totalSize = file.size;
  let offset = 0;
  let bytesSentForSpeed = 0;
  let lastSpeedTime = Date.now();

  hideError("sender-transfer-error");
  showStep("screen-sender", "sender-step-transfer");

  // Metadados: envia primeiro como JSON string
  dc.send(
    JSON.stringify({
      name: file.name,
      size: file.size,
      type: file.type || "application/octet-stream",
    }),
  );

  // Use bufferedAmountLowThreshold + event instead of a polling loop.
  dc.bufferedAmountLowThreshold = BUFFER_THRESHOLD;

  while (offset < totalSize) {
    // If the buffer is above the threshold, wait for the browser to drain it.
    if (dc.bufferedAmount > BUFFER_THRESHOLD) {
      await new Promise((resolve) => {
        dc.addEventListener("bufferedamountlow", resolve, { once: true });
      });
    }

    const slice = file.slice(offset, offset + CHUNK_SIZE);
    const buffer = await slice.arrayBuffer();
    dc.send(buffer);
    offset += buffer.byteLength;
    bytesSentForSpeed += buffer.byteLength;

    // Atualiza progresso
    const pct = Math.min(100, Math.round((offset / totalSize) * 100));
    $("sender-progress-fill").style.width = pct + "%";
    $("sender-progress-pct").textContent = pct + "%";

    // Velocidade + ETA (atualiza a cada 250ms)
    const now = Date.now();
    const elapsed = now - lastSpeedTime;
    if (elapsed >= 250) {
      const bps = (bytesSentForSpeed / elapsed) * 1000;
      $("sender-speed").textContent = formatSpeed(bps);
      if (bps > 0) {
        const remaining = totalSize - offset;
        $("sender-eta").textContent = formatETA(remaining / bps);
      }
      bytesSentForSpeed = 0;
      lastSpeedTime = now;
    }
  }

  // Envio concluído — aguarda um tick para o buffer drenar
  await new Promise((resolve) => setTimeout(resolve, 200));
  dc.send(JSON.stringify({ done: true }));

  $("sender-eta").textContent = "";
  $("sender-done-info").textContent =
    `${file.name} (${formatBytes(file.size)}) enviado com sucesso.`;
  showStep("screen-sender", "sender-step-done");
}

// ════════════════════════════════════════════════════════════════════════════
//  RECEIVER
// ════════════════════════════════════════════════════════════════════════════

async function startReceiver(roomCode) {
  $("receiver-status").textContent = "Conectando ao servidor...";
  hideError("receiver-ws-error");
  showStep("screen-receiver", "receiver-step-wait");

  try {
    _sig = new Signaling(onReceiverSignal, () => {
      // WS fechou inesperadamente
      showError("receiver-ws-error", "Conexão com o servidor perdida. Tente novamente.");
      $("receiver-status").textContent = "";
      $("receiver-spinner").style.display = "none";
    });
    await _sig.ready();
  } catch (err) {
    showStep("screen-receiver", "receiver-step-code");
    showError("receiver-error", "Não foi possível conectar ao servidor.");
    resetState();
    return;
  }

  _sig.send({ type: "join", room: roomCode, role: "receiver" });

  function onReceiverSignal(msg) {
    switch (msg.type) {
      case "joined":
        $("receiver-status").textContent =
          "Conectado. Aguardando oferta do remetente...";
        break;

      case "offer":
        $("receiver-status").textContent =
          "Remetente encontrado. Estabelecendo conexão P2P...";
        initReceiverPeer(msg.payload);
        break;

      case "ice":
        if (_pc && _pc.remoteDescription) {
          _pc.addIceCandidate(new RTCIceCandidate(msg.payload)).catch(console.warn);
        } else {
          _pendingCandidates.push(msg.payload);
        }
        break;

      case "peer_left":
        console.warn("[receiver] remetente desconectou");
        const transferStep = document.querySelector("#receiver-step-transfer.active");
        if (transferStep) {
          showError("receiver-transfer-error", "O remetente desconectou durante a transferência.");
        }
        break;

      case "error": {
        const reason = msg.payload;
        const msgs = {
          room_not_found: "Sala não encontrada. Verifique as 3 palavras.",
          room_full: "Sala já está ocupada.",
          invalid_join: "Código inválido.",
        };
        showStep("screen-receiver", "receiver-step-code");
        showError("receiver-error", msgs[reason] || `Erro: ${reason}`);
        resetState();
        break;
      }
    }
  }
}

function initReceiverPeer(offerSDP) {
  _pc = createPeer((candidate) => {
    _sig.send({ type: "ice", payload: candidate });
  });

  // Receptor aguarda o DataChannel abrir
  _pc.addEventListener("datachannel", (e) => {
    const dc = e.channel;
    dc.binaryType = "arraybuffer";
    setupReceiverChannel(dc);
  });

  _pc
    .setRemoteDescription(new RTCSessionDescription(offerSDP))
    .then(() => {
      // Flush any ICE candidates queued before the remote description was set.
      const queued = _pendingCandidates.splice(0);
      return Promise.all(
        queued.map((c) => _pc.addIceCandidate(new RTCIceCandidate(c)).catch(console.warn))
      );
    })
    .then(() => _pc.createAnswer())
    .then((answer) => _pc.setLocalDescription(answer))
    .then(() => _sig.send({ type: "answer", payload: _pc.localDescription }))
    .catch(console.error);
}

function setupReceiverChannel(dc) {
  let meta = null; // { name, size, type }
  const chunks = [];
  let received = 0;
  let bytesSoFar = 0;
  let lastSpeedTime = Date.now();

  dc.addEventListener("close", () => {
    // DataChannel fechou antes de receber { done: true }
    const transferStep = document.querySelector("#receiver-step-transfer.active");
    if (transferStep) {
      showError("receiver-transfer-error", "A conexão com o remetente foi encerrada antes do término.");
    }
  });

  dc.addEventListener("message", (e) => {
    const data = e.data;

    // Mensagem de controle (string JSON)
    if (typeof data === "string") {
      const obj = JSON.parse(data);
      if (obj.done) {
        // Transferência completa
        const blob = new Blob(chunks, {
          type: meta ? meta.type : "application/octet-stream",
        });
        _receivedBlob = blob;
        const fileName = meta ? meta.name : "arquivo";

        $("receiver-eta").textContent = "";
        $("receiver-done-info").textContent =
          `${fileName} (${formatBytes(blob.size)}) recebido.`;

        // Auto-download
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = fileName;
        a.click();
        // Revoke promptly — 1 s is enough for the browser to start the download.
        setTimeout(() => URL.revokeObjectURL(url), 1000);

        // Botão de download manual como fallback
        $("receiver-btn-download").onclick = () => {
          const url2 = URL.createObjectURL(blob);
          const a2 = document.createElement("a");
          a2.href = url2;
          a2.download = fileName;
          a2.click();
          // Revoke after a short delay so the download starts before cleanup.
          setTimeout(() => URL.revokeObjectURL(url2), 1000);
        };
        showStep("screen-receiver", "receiver-step-done");
        return;
      }
      if (obj.name !== undefined) {
        meta = obj;
        setFileInfo("receiver-file-info", meta.name, meta.size);
        hideError("receiver-transfer-error");
        showStep("screen-receiver", "receiver-step-transfer");
        lastSpeedTime = Date.now();
        return;
      }
    }

    // Chunk binário
    chunks.push(data);
    received += data.byteLength;
    bytesSoFar += data.byteLength;

    if (meta) {
      const pct = Math.min(100, Math.round((received / meta.size) * 100));
      $("receiver-progress-fill").style.width = pct + "%";
      $("receiver-progress-pct").textContent = pct + "%";
    }

    // Velocidade + ETA
    const now = Date.now();
    const elapsed = now - lastSpeedTime;
    if (elapsed >= 250) {
      const bps = (bytesSoFar / elapsed) * 1000;
      $("receiver-speed").textContent = formatSpeed(bps);
      if (meta && bps > 0) {
        const remaining = meta.size - received;
        $("receiver-eta").textContent = formatETA(remaining / bps);
      }
      bytesSoFar = 0;
      lastSpeedTime = now;
    }
  });

  dc.addEventListener("error", (e) => console.error("[receiver] DC error", e));
}

// ════════════════════════════════════════════════════════════════════════════
//  AUTO-PREENCHIMENTO VIA URL (?room=palavra1-palavra2-palavra3)
// ════════════════════════════════════════════════════════════════════════════
(function checkRoomParam() {
  const params = new URLSearchParams(location.search);
  const room = params.get("room");
  if (!room) return;
  const parts = room.split("-");
  if (parts.length !== 3) return;
  // Navega para tela de receptor e preenche os campos
  const [w1, w2, w3] = parts.map((w) => w.trim().toLowerCase());
  if (!w1 || !w2 || !w3) return;
  $("word1").value = w1;
  $("word2").value = w2;
  $("word3").value = w3;
  hideError("receiver-error");
  showStep("screen-receiver", "receiver-step-code");
  showScreen("screen-receiver");
  // Remove o parâmetro da URL sem recarregar
  history.replaceState({}, "", location.pathname);
})();

// ════════════════════════════════════════════════════════════════════════════
//  EVENT LISTENERS
// ════════════════════════════════════════════════════════════════════════════

// Tela inicial
$("btn-send").addEventListener("click", () => {
  resetState();
  showStep("screen-sender", "sender-step-file");
  showScreen("screen-sender");
});

$("btn-receive").addEventListener("click", () => {
  resetState();
  $("word1").value = "";
  $("word2").value = "";
  $("word3").value = "";
  hideError("receiver-error");
  showStep("screen-receiver", "receiver-step-code");
  showScreen("screen-receiver");
  $("word1").focus();
});

// Voltar
$("sender-back").addEventListener("click", () => {
  resetState();
  showScreen("screen-home");
});

$("receiver-back").addEventListener("click", () => {
  resetState();
  showScreen("screen-home");
});

// Drop zone / seleção de arquivo
const dropZone = document.querySelector(".drop-zone");
const fileInput = $("file-input");

dropZone.addEventListener("click", (e) => {
  if (e.target.closest("label, input")) return;
  fileInput.click();
});

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("drag-over");
});
dropZone.addEventListener("dragleave", () =>
  dropZone.classList.remove("drag-over"),
);
dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("drag-over");
  const f = e.dataTransfer.files[0];
  if (f) startSender(f);
});

fileInput.addEventListener("change", () => {
  if (fileInput.files[0]) startSender(fileInput.files[0]);
  fileInput.value = "";
});

// Copiar código — com feedback de cor
$("btn-copy-code").addEventListener("click", () => {
  const code = $("room-code-display").textContent;
  navigator.clipboard.writeText(code).then(() => {
    const btn = $("btn-copy-code");
    btn.textContent = "Copiado!";
    btn.classList.add("copied");
    setTimeout(() => {
      btn.textContent = "Copiar código";
      btn.classList.remove("copied");
    }, 2000);
  });
});

// Copiar link — copia a URL completa da sala
$("btn-copy-link").addEventListener("click", () => {
  if (!_roomURL) return;
  navigator.clipboard.writeText(_roomURL).then(() => {
    const btn = $("btn-copy-link");
    btn.textContent = "Copiado!";
    btn.classList.add("copied");
    setTimeout(() => {
      btn.textContent = "Copiar link";
      btn.classList.remove("copied");
    }, 2000);
  });
});

// Conectar receptor
$("btn-connect").addEventListener("click", connectReceiver);

// Enter em qualquer campo de palavra avança / conecta
// Espaço avança para o próximo campo
const WORD_IDS = ["word1", "word2", "word3"];

WORD_IDS.forEach((id, i) => {
  const el = $(id);

  el.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      if (i < 2) $(WORD_IDS[i + 1]).focus();
      else connectReceiver();
    }
    if (e.key === " " && i < 2) {
      e.preventDefault();
      $(WORD_IDS[i + 1]).focus();
    }
  });

  // Auto-avanço: quando a palavra digitada pertence ao BIP39, avança
  el.addEventListener("input", () => {
    if (i >= 2) return; // último campo: não avança
    const val = el.value.trim().toLowerCase();
    if (val.length >= 3 && BIP39_SET.has(val)) {
      $(WORD_IDS[i + 1]).focus();
      $(WORD_IDS[i + 1]).select();
    }
  });
});

function connectReceiver() {
  hideError("receiver-error");
  const w1 = $("word1").value.trim().toLowerCase();
  const w2 = $("word2").value.trim().toLowerCase();
  const w3 = $("word3").value.trim().toLowerCase();
  if (!w1 || !w2 || !w3) {
    showError("receiver-error", "Preencha as 3 palavras.");
    return;
  }
  const roomCode = `${w1}-${w2}-${w3}`;
  startReceiver(roomCode);
}

// Nova transferência (sender)
$("sender-btn-new").addEventListener("click", () => {
  resetState();
  showStep("screen-sender", "sender-step-file");
  fileInput.value = "";
});

// Nova transferência (receiver)
$("receiver-btn-new").addEventListener("click", () => {
  resetState();
  $("word1").value = "";
  $("word2").value = "";
  $("word3").value = "";
  hideError("receiver-error");
  showStep("screen-receiver", "receiver-step-code");
  $("word1").focus();
});
