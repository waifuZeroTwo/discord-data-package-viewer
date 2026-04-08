const path = require('path')
const nodeFs = require('fs')
const fs = require('fs/promises')
const crypto = require('crypto')
const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const extract = require('extract-zip')
const {
  parseDiscordExport,
  getPaginatedParserOutput,
} = require('./src/main/parser/discordExport')
const { IMPORT_STATES } = require('./src/shared/importStates')

const IMPORTS_SUBDIR = 'imports'
const IMPORT_RETENTION_MS = 1000 * 60 * 60 * 24 * 7
const IMPORT_MAX_COUNT = 10
const PARSER_DEBUG_LOG = 'parser-debug.log'
const ANALYTICS_CACHE_SUBDIR = 'analytics-cache'
const parsedArchiveCache = new Map()
const activeParserJobs = new Map()
let activeImportRequest = null

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
    title: 'Discord Analytics Dashboard',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  })

  win.loadFile(path.join(__dirname, 'src/renderer/index.html'))
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

function emitImportStatus(event, payload) {
  if (!event?.sender || event.sender.isDestroyed()) {
    return
  }
  event.sender.send('import:status', payload)
}

async function handleSelectZip(event, payload = {}) {
  const importId = payload.importId || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  if (activeImportRequest && activeImportRequest !== importId) {
    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.FAILED,
      message: `Another import (${activeImportRequest}) is already running.`,
    })
    return {
      ok: false,
      importId,
      busy: true,
      warnings: ['Another import is already in progress. Please wait for it to finish.'],
      detectedSections: [],
      parserSummary: {
        channelCount: 0,
        messageCount: 0,
      },
    }
  }

  activeImportRequest = importId
  emitImportStatus(event, {
    importId,
    state: IMPORT_STATES.SELECTING_FILE,
    message: 'Waiting for ZIP file selection.',
  })

  try {
    const { canceled, filePaths } = await dialog.showOpenDialog({
      title: 'Select Discord Data Package',
      properties: ['openFile'],
      filters: [
        { name: 'ZIP archive', extensions: ['zip'] },
        { name: 'All files', extensions: ['*'] },
      ],
    })

    if (canceled || filePaths.length === 0) {
      emitImportStatus(event, {
        importId,
        state: IMPORT_STATES.CANCELED,
        message: 'Import canceled before file selection.',
      })
      return null
    }

    const warnings = []
    const selectedZipPath = filePaths[0]
    const importsRoot = path.join(app.getPath('userData'), IMPORTS_SUBDIR)
    const analyticsCacheRoot = path.join(app.getPath('userData'), ANALYTICS_CACHE_SUBDIR)

    await cleanupOldImports(importsRoot)
    await fs.mkdir(analyticsCacheRoot, { recursive: true })

    const rootPath = path.join(importsRoot, importId)
    const importHash = await computeFileSha256(selectedZipPath)
    const cachedAnalytics = await loadAnalyticsCache(analyticsCacheRoot, importHash)

    await fs.mkdir(rootPath, { recursive: true })

    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.EXTRACTING_ZIP,
      message: 'Extracting ZIP archive.',
    })
    await extract(selectedZipPath, { dir: rootPath })

    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.SCANNING_FILES,
      message: 'Scanning extracted files for Discord sections.',
    })
    const detectedSections = await detectSections(rootPath)
    const expectedSections = KNOWN_SECTION_DETECTORS.map((item) => item.name)
    const missingSections = expectedSections.filter((section) => !detectedSections.includes(section))

    if (detectedSections.length === 0) {
      warnings.push(
        'No known Discord data sections were detected. Results are likely very limited and may be incomplete.',
      )
    } else if (missingSections.length > 0) {
      warnings.push(
        `Partial import: some expected export sections were not found (${missingSections.join(', ')}). Available sections were parsed.`,
      )
    }

    let parsedExport
    if (cachedAnalytics) {
      emitImportStatus(event, {
        importId,
        state: IMPORT_STATES.AGGREGATING,
        message: 'Using cached analytics.',
      })
      parsedExport = cachedAnalytics
      if (event?.sender && !event.sender.isDestroyed()) {
        event.sender.send('parser:progress', {
          importId,
          importHash,
          stage: 'complete',
          filesScanned: 0,
          totalFiles: 0,
          recordsProcessed: parsedExport.analyticsSummary?.messageCount ?? 0,
          currentPath: null,
          etaSeconds: 0,
          elapsedMs: 0,
          cacheHit: true,
        })
      }
    } else {
      emitImportStatus(event, {
        importId,
        state: IMPORT_STATES.PARSING,
        message: 'Parsing Discord export files.',
      })
      const parseAbortController = new AbortController()
      activeParserJobs.set(importId, { abortController: parseAbortController })
      try {
        parsedExport = await parseDiscordExport(rootPath, {
          sortDirection: 'asc',
          signal: parseAbortController.signal,
          onProgress: (progress) => {
            if (event?.sender && !event.sender.isDestroyed()) {
              event.sender.send('parser:progress', {
                importId,
                importHash,
                ...progress,
              })
            }
          },
        })
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.AGGREGATING,
          message: 'Aggregating analytics output.',
        })
        await persistAnalyticsCache(analyticsCacheRoot, importHash, parsedExport)
      } catch (error) {
        if (error?.name === 'AbortError') {
          await fs.rm(rootPath, { recursive: true, force: true })
          emitImportStatus(event, {
            importId,
            state: IMPORT_STATES.CANCELED,
            message: 'Import canceled by user.',
          })
          return {
            ok: false,
            canceled: true,
            importId,
            importHash,
            rootPath,
            warnings: ['Import cancelled before analytics indexing finished.'],
            detectedSections: [],
            parserSummary: {
              channelCount: 0,
              messageCount: 0,
            },
          }
        }

        throw error
      } finally {
        activeParserJobs.delete(importId)
      }
    }

    await appendParserDebugLog(importId, [...warnings, ...parsedExport.warnings])
    parsedArchiveCache.set(importId, parsedExport)

    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.COMPLETED,
      message: 'Import completed successfully.',
    })
    return {
      ok: true,
      importId,
      importHash,
      cacheHit: Boolean(cachedAnalytics),
      rootPath,
      warnings: [...warnings, ...parsedExport.warnings],
      detectedSections,
      missingSections,
      parserSummary: {
        channelCount: parsedExport.analyticsSummary.channelCount,
        messageCount: parsedExport.analyticsSummary.messageCount,
        sortDirection: parsedExport.sortDirection,
      },
    }
  } catch (error) {
    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.FAILED,
      message: `Import failed: ${error.message}`,
    })
    return {
      ok: false,
      importId,
      warnings: [`Import failed: ${error.message}`],
      detectedSections: [],
      parserSummary: {
        channelCount: 0,
        messageCount: 0,
      },
    }
  } finally {
    if (activeImportRequest === importId) {
      activeImportRequest = null
    }
  }
}

async function computeFileSha256(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256')
    const stream = nodeFs.createReadStream(filePath)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('end', () => resolve(hash.digest('hex')))
    stream.on('error', reject)
  })
}

async function loadAnalyticsCache(cacheRoot, importHash) {
  const cachePath = path.join(cacheRoot, `${importHash}.json`)
  try {
    const payload = await fs.readFile(cachePath, 'utf8')
    const parsed = JSON.parse(payload)
    if (!parsed || typeof parsed !== 'object') {
      return null
    }

    return parsed
  } catch {
    return null
  }
}

async function persistAnalyticsCache(cacheRoot, importHash, parsedExport) {
  const cachePath = path.join(cacheRoot, `${importHash}.json`)
  await fs.writeFile(cachePath, JSON.stringify(parsedExport), 'utf8')
}

async function appendParserDebugLog(importId, warningMessages) {
  if (!Array.isArray(warningMessages) || warningMessages.length === 0) {
    return
  }

  const logPath = path.join(app.getPath('userData'), PARSER_DEBUG_LOG)
  const timestamp = new Date().toISOString()
  const lines = [
    `=== ${timestamp} import:${importId} ===`,
    ...warningMessages.map((message) => `WARN ${message}`),
    '',
  ]

  try {
    await fs.appendFile(logPath, `${lines.join('\n')}\n`, 'utf8')
  } catch (error) {
    console.warn(`Failed to write parser debug log at ${logPath}:`, error)
  }
}

async function handleGetParserAnalytics(_event, payload = {}) {
  const importId = payload.importId
  const parsedExport = getCachedParse(importId)

  if (!parsedExport) {
    return {
      ok: false,
      warnings: [
        `No parsed archive found for import ${importId}. Re-import the package before requesting analytics.`,
      ],
    }
  }

  return {
    ok: true,
    importId,
    ...getPaginatedParserOutput(parsedExport, payload),
  }
}

function handleCancelParserJob(_event, payload = {}) {
  const importId = payload.importId
  if (!importId || !activeParserJobs.has(importId)) {
    return { ok: false, canceled: false }
  }

  activeParserJobs.get(importId).abortController.abort()
  return { ok: true, canceled: true }
}

app.whenReady().then(() => {
  ipcMain.handle('dialog:pick-data-package', handleSelectZip)
  ipcMain.handle('dialog:select-zip', handleSelectZip)
  ipcMain.handle('parser:get-analytics', handleGetParserAnalytics)
  ipcMain.handle('parser:cancel-job', handleCancelParserJob)

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
