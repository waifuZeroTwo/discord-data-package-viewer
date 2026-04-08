const path = require('path')
const fs = require('fs/promises')
const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const extract = require('extract-zip')
const {
  parseDiscordExport,
  getPaginatedParserOutput,
} = require('./src/main/parser/discordExport')

const IMPORTS_SUBDIR = 'imports'
const IMPORT_RETENTION_MS = 1000 * 60 * 60 * 24 * 7
const IMPORT_MAX_COUNT = 10
const parsedArchiveCache = new Map()

const KNOWN_SECTION_DETECTORS = [
  {
    name: 'messages',
    anyPaths: ['messages', path.join('messages', 'index.json')],
  },
  {
    name: 'account',
    anyPaths: [
      path.join('account', 'profile.json'),
      path.join('account', 'account.json'),
      path.join('account', 'user.json'),
    ],
  },
  {
    name: 'servers',
    anyPaths: ['servers', path.join('servers', 'index.json')],
  },
  {
    name: 'relationships',
    anyPaths: [
      'relationships',
      path.join('relationships', 'friends.json'),
      path.join('relationships', 'blocked.json'),
    ],
  },
]

function createWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 960,
    minHeight: 640,
    title: 'Discord Data Package Viewer',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })

  win.loadFile(path.join(__dirname, 'src/renderer/index.html'))
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0
}

function sanitizePositiveInteger(value, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const numericValue = Number(value)
  if (!Number.isInteger(numericValue)) {
    return fallback
  }

  if (numericValue < min || numericValue > max) {
    return fallback
  }

  return numericValue
}

function sanitizeDateFilter(value) {
  if (!isNonEmptyString(value)) {
    return null
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return null
  }

  return value
}

function sanitizeParserPayload(payload = {}) {
  if (!payload || typeof payload !== 'object') {
    return {
      valid: false,
      warnings: ['Invalid parser payload. Expected an object.'],
    }
  }

  const importId = isNonEmptyString(payload.importId) ? payload.importId.trim() : ''
  if (!importId) {
    return {
      valid: false,
      warnings: ['Invalid parser payload: importId is required.'],
    }
  }

  const channelId = isNonEmptyString(payload.channelId) ? payload.channelId.trim() : undefined
  const sortDirection = payload.sortDirection === 'desc' ? 'desc' : 'asc'

  const sanitizedPayload = {
    importId,
    sortDirection,
    channelId,
    channelPage: sanitizePositiveInteger(payload.channelPage, 1, 1, 10000),
    channelPageSize: sanitizePositiveInteger(payload.channelPageSize, 100, 1, 5000),
    messagePage: sanitizePositiveInteger(payload.messagePage, 1, 1, 100000),
    messagePageSize: sanitizePositiveInteger(payload.messagePageSize, 100, 1, 20000),
    includeMessages: payload.includeMessages !== false,
  }

  if (isNonEmptyString(payload.searchQuery)) {
    sanitizedPayload.searchQuery = payload.searchQuery.trim().slice(0, 200)
  }

  if (Array.isArray(payload.authors)) {
    sanitizedPayload.authors = payload.authors
      .filter(isNonEmptyString)
      .map((author) => author.trim().slice(0, 100))
      .slice(0, 100)
  }

  if (Array.isArray(payload.hasFlags)) {
    sanitizedPayload.hasFlags = payload.hasFlags
      .filter(isNonEmptyString)
      .map((flag) => flag.trim().slice(0, 64))
      .slice(0, 50)
  }

  const fromDate = sanitizeDateFilter(payload.fromDate)
  if (fromDate) {
    sanitizedPayload.fromDate = fromDate
  }

  const toDate = sanitizeDateFilter(payload.toDate)
  if (toDate) {
    sanitizedPayload.toDate = toDate
  }

  return {
    valid: true,
    payload: sanitizedPayload,
  }
}

async function pathExists(targetPath) {
  try {
    await fs.access(targetPath)
    return true
  } catch {
    return false
  }
}

async function cleanupOldImports(importsRoot) {
  await fs.mkdir(importsRoot, { recursive: true })

  const importEntries = await fs.readdir(importsRoot, { withFileTypes: true })
  const importDirs = []

  for (const entry of importEntries) {
    if (!entry.isDirectory()) {
      continue
    }

    const fullPath = path.join(importsRoot, entry.name)
    const stats = await fs.stat(fullPath)
    importDirs.push({
      fullPath,
      mtimeMs: stats.mtimeMs,
      isExpired: Date.now() - stats.mtimeMs > IMPORT_RETENTION_MS,
    })
  }

  for (const importDir of importDirs.filter((item) => item.isExpired)) {
    await fs.rm(importDir.fullPath, { recursive: true, force: true })
    parsedArchiveCache.delete(path.basename(importDir.fullPath))
  }

  const recentImports = importDirs.filter((item) => !item.isExpired)
  if (recentImports.length <= IMPORT_MAX_COUNT) {
    return
  }

  recentImports.sort((a, b) => a.mtimeMs - b.mtimeMs)
  const dirsToDelete = recentImports.slice(0, recentImports.length - IMPORT_MAX_COUNT)
  for (const importDir of dirsToDelete) {
    await fs.rm(importDir.fullPath, { recursive: true, force: true })
    parsedArchiveCache.delete(path.basename(importDir.fullPath))
  }
}

async function detectSections(rootPath) {
  const detectedSections = []

  for (const detector of KNOWN_SECTION_DETECTORS) {
    for (const relativePath of detector.anyPaths) {
      if (await pathExists(path.join(rootPath, relativePath))) {
        detectedSections.push(detector.name)
        break
      }
    }
  }

  return detectedSections
}

function getCachedParse(importId) {
  if (!importId || !parsedArchiveCache.has(importId)) {
    return null
  }

  return parsedArchiveCache.get(importId)
}

async function handleSelectZip() {
  const { canceled, filePaths } = await dialog.showOpenDialog({
    title: 'Select Discord Data Package',
    properties: ['openFile'],
    filters: [
      { name: 'ZIP archive', extensions: ['zip'] },
      { name: 'All files', extensions: ['*'] },
    ],
  })

  if (canceled || filePaths.length === 0) {
    return null
  }

  const warnings = []
  const selectedZipPath = filePaths[0]
  if (!isNonEmptyString(selectedZipPath) || path.extname(selectedZipPath).toLowerCase() !== '.zip') {
    return {
      ok: false,
      importId: null,
      rootPath: null,
      warnings: ['Selected file must be a .zip archive.'],
      detectedSections: [],
      parserSummary: {
        channelCount: 0,
        messageCount: 0,
      },
    }
  }

  const importsRoot = path.join(app.getPath('userData'), IMPORTS_SUBDIR)

  await cleanupOldImports(importsRoot)

  const importId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  const rootPath = path.join(importsRoot, importId)

  await fs.mkdir(rootPath, { recursive: true })

  try {
    await extract(selectedZipPath, { dir: rootPath })
  } catch (error) {
    await fs.rm(rootPath, { recursive: true, force: true })
    return {
      ok: false,
      importId,
      rootPath,
      warnings: [`Failed to extract ZIP archive: ${error.message}`],
      detectedSections: [],
      parserSummary: {
        channelCount: 0,
        messageCount: 0,
      },
    }
  }

  const detectedSections = await detectSections(rootPath)

  if (detectedSections.length === 0) {
    warnings.push(
      'No known Discord data sections were detected (expected directories like messages or account).',
    )
  }

  const parsedExport = await parseDiscordExport(rootPath, { sortDirection: 'asc' })
  parsedArchiveCache.set(importId, parsedExport)

  const messageCount = Object.values(parsedExport.messagesByChannel).reduce(
    (count, messages) => count + messages.length,
    0,
  )

  return {
    ok: detectedSections.length > 0,
    importId,
    rootPath,
    warnings: [...warnings, ...parsedExport.warnings],
    detectedSections,
    parserSummary: {
      channelCount: parsedExport.channels.length,
      messageCount,
      sortDirection: parsedExport.sortDirection,
    },
  }
}

async function handleGetParserPage(_event, payload = {}) {
  const sanitizedPayloadResult = sanitizeParserPayload(payload)
  if (!sanitizedPayloadResult.valid) {
    return {
      ok: false,
      warnings: sanitizedPayloadResult.warnings,
      channels: [],
      messagesByChannel: {},
    }
  }

  const sanitizedPayload = sanitizedPayloadResult.payload
  const importId = sanitizedPayload.importId
  const parsedExport = getCachedParse(importId)

  if (!parsedExport) {
    return {
      ok: false,
      warnings: [
        `No parsed archive found for import ${importId}. Re-import the package before requesting parser pages.`,
      ],
      channels: [],
      messagesByChannel: {},
    }
  }

  if (sanitizedPayload.sortDirection !== parsedExport.sortDirection) {
    const sortedExport = {
      ...parsedExport,
      messagesByChannel: { ...parsedExport.messagesByChannel },
      sortDirection: sanitizedPayload.sortDirection,
    }

    for (const [channelId, messages] of Object.entries(sortedExport.messagesByChannel)) {
      sortedExport.messagesByChannel[channelId] = [...messages].sort((a, b) => {
        const aEpoch = Number.isNaN(a.timestampEpochMs) ? Number.MAX_SAFE_INTEGER : a.timestampEpochMs
        const bEpoch = Number.isNaN(b.timestampEpochMs) ? Number.MAX_SAFE_INTEGER : b.timestampEpochMs

        if (aEpoch === bEpoch) {
          return 0
        }

        if (sortedExport.sortDirection === 'desc') {
          return aEpoch < bEpoch ? 1 : -1
        }

        return aEpoch > bEpoch ? 1 : -1
      })
    }

    parsedArchiveCache.set(importId, sortedExport)
  }

  const currentParse = getCachedParse(importId)

  return {
    ok: true,
    importId,
    sortDirection: currentParse.sortDirection,
    ...getPaginatedParserOutput(currentParse, sanitizedPayload),
  }
}

app.whenReady().then(() => {
  ipcMain.handle('dialog:pick-data-package', handleSelectZip)
  ipcMain.handle('dialog:select-zip', handleSelectZip)
  ipcMain.handle('parser:get-page', handleGetParserPage)

  createWindow()

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow()
    }
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit()
  }
})
