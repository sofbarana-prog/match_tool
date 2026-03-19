# Funding Call Matcher — Deploy su GitHub

## Architettura

```
GitHub Actions (cron ogni notte alle 03:00 UTC)
    └─▶ fetch_calls.js  chiama l'API EU e scrive calls.json
    └─▶ commit + push automatico nel repo

GitHub Pages
    └─▶ index.html  legge calls.json (aggiornato la notte prima)
```

Nessun server, nessun database, nessun costo. Tutto gira su GitHub gratuitamente.

---

## Struttura del repo

```
/
├── index.html                       ← rinomina tool.html → index.html
├── calls.json                       ← generato automaticamente dall'Action
├── fetch_calls.js                   ← script di fetch (gira nell'Action)
└── .github/
    └── workflows/
        └── update_calls.yml         ← definizione dell'Action
```

---

## Setup (~10 minuti)

### 1. Carica i file nel repo

Carica nel tuo repo GitHub:
- `tool.html` → rinominato come **`index.html`**
- `fetch_calls.js`
- `.github/workflows/update_calls.yml` (rispetta la struttura di cartelle)

Puoi farlo trascinando i file su github.com → **Add file → Upload files**,
oppure da terminale con `git add / commit / push`.

### 2. Abilita GitHub Pages

1. **Settings** → **Pages**
2. Source: `Deploy from a branch` → branch `main` → cartella `/ (root)`
3. **Save** — dopo 1-2 minuti il sito è live su `https://tuousername.github.io/tuorepo`

### 3. Abilita i permessi per l'Action

1. **Settings** → **Actions** → **General**
2. Scorri fino a **Workflow permissions**
3. Seleziona **Read and write permissions** → **Save**

Questo permette all'Action di fare commit di `calls.json` nel repo.

### 4. Genera il primo calls.json

La prima volta lancia l'Action manualmente invece di aspettare la notte:

1. **Actions** → **Update EU Calls** → **Run workflow** → **Run workflow**
2. Attendi 2-5 minuti (scarica ~1000 call dal portale EU)
3. Controlla che sia apparso `calls.json` nel repo (tab **Code**)
4. Il sito mostra subito i dati con la data di aggiornamento

---

## Aggiornamento automatico

L'Action gira ogni notte alle **03:00 UTC** (05:00 ora italiana in estate).

- Nuove call trovate → aggiorna `calls.json` e fa commit automatico
- Nulla cambiato → nessun commit (git diff --staged --quiet)

Per cambiare l'orario, modifica `cron` in `update_calls.yml`:
```yaml
- cron: "0 3 * * *"   # 03:00 UTC ogni giorno
```
Generatore visivo: https://crontab.guru

---

## Aggiornamento manuale

**Actions** → **Update EU Calls** → **Run workflow** — in qualsiasi momento.

---

## Verifica

Dopo il primo run, apri il sito: in cima al form comparirà
> *Dati aggiornati al: GG/MM/AAAA HH:MM UTC*

Se non compare: controlla che `calls.json` esista nel repo e che l'Action
(tab **Actions**) mostri un pallino verde.
