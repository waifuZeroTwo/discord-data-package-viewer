const path = require('path')
const nodeFs = require('fs')
const fs = require('fs/promises')
const crypto = require('crypto')
const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const extract = require('extract-zip')
const yauzl = require('yauzl')
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
const DEFAULT_PHASE_TIMEOUTS_MS = Object.freeze({
  extracting: 1000 * 60 * 5,
  scanning: 1000 * 60,
  parsing: 1000 * 60 * 10,
  aggregating: 1000 * 60 * 2,
})
const DEFAULT_STALL_THRESHOLD_MS = 1000 * 15
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

function normalizeProgressPayload(payload = {}) {
  const filesTotal = Number.isFinite(payload.filesTotal) ? payload.filesTotal : null
  const filesDone = Number.isFinite(payload.filesDone) ? payload.filesDone : null
  const percentFromPayload = Number.isFinite(payload.percent) ? payload.percent : null
  const computedPercent =
    filesTotal && filesTotal > 0 && Number.isFinite(filesDone)
      ? Math.max(0, Math.min(100, Math.round((filesDone / filesTotal) * 100)))
      : null

  return {
    phase: typeof payload.phase === 'string' ? payload.phase : 'unknown',
    percent: percentFromPayload ?? computedPercent,
    filesTotal,
    filesDone,
    recordsDone: Number.isFinite(payload.recordsDone) ? payload.recordsDone : null,
    message: typeof payload.message === 'string' ? payload.message : '',
    updatedAt: new Date().toISOString(),
    heartbeat: Boolean(payload.heartbeat),
  }
}

function emitParserProgress(event, meta, payload) {
  if (!event?.sender || event.sender.isDestroyed()) {
    return
  }
  event.sender.send('parser:progress', {
    importId: meta.importId,
    importHash: meta.importHash,
    progress: normalizeProgressPayload(payload),
  })
}

function nowIso() {
  return new Date().toISOString()
}

function createImportRuntime(importId, importHash, options = {}) {
  const phaseTimeoutsMs = { ...DEFAULT_PHASE_TIMEOUTS_MS, ...(options.phaseTimeoutsMs || {}) }
  const stallThresholdMs = Number.isFinite(options.stallThresholdMs)
    ? Math.max(1000, options.stallThresholdMs)
    : DEFAULT_STALL_THRESHOLD_MS

  return {
    importId,
    importHash,
    importSourceType: options.importSourceType === 'folder' ? 'folder' : 'zip',
    selectedSourcePath: options.selectedSourcePath || null,
    phaseTimeoutsMs,
    stallThresholdMs,
    currentPhase: 'selecting',
    phaseStartedAtMs: Date.now(),
    lastProgressAtMs: Date.now(),
    stalledAtMs: null,
    stallCount: 0,
    canceled: false,
    diagnostics: [],
    monitorInterval: null,
    parseAbortController: null,
  }
}

function recordDiagnostic(runtime, entry) {
  runtime.diagnostics.push({
    recordedAt: nowIso(),
    ...entry,
  })
  if (runtime.diagnostics.length > 30) {
    runtime.diagnostics.shift()
  }
}

function startRuntimeMonitor(event, runtime) {
  stopRuntimeMonitor(runtime)
  runtime.monitorInterval = setInterval(() => {
    const phase = runtime.currentPhase
    if (!phase || phase === 'complete') {
      return
    }

    const phaseTimeoutMs = runtime.phaseTimeoutsMs[phase] ?? DEFAULT_PHASE_TIMEOUTS_MS.parsing
    const elapsedMs = Date.now() - runtime.phaseStartedAtMs
    const silentForMs = Date.now() - runtime.lastProgressAtMs
    const isStalled = silentForMs >= runtime.stallThresholdMs || elapsedMs >= phaseTimeoutMs

    if (!isStalled || runtime.stalledAtMs) {
      return
    }

    runtime.stalledAtMs = Date.now()
    runtime.stallCount += 1
    const diagnostic = {
      type: 'stall',
      phase,
      elapsedMs,
      silentForMs,
      thresholdMs: runtime.stallThresholdMs,
      maxDurationMs: phaseTimeoutMs,
      importId: runtime.importId,
      importSourceType: runtime.importSourceType,
      sourcePath: runtime.selectedSourcePath,
    }
    recordDiagnostic(runtime, diagnostic)

    emitImportStatus(event, {
      importId: runtime.importId,
      state: IMPORT_STATES.STALLED,
      message: `Import appears stalled during ${phase} after ${Math.round(elapsedMs / 1000)}s.`,
      stall: diagnostic,
    })
  }, 1000)
}

function stopRuntimeMonitor(runtime) {
  if (runtime?.monitorInterval) {
    clearInterval(runtime.monitorInterval)
    runtime.monitorInterval = null
  }
}

function setRuntimePhase(runtime, phase) {
  runtime.currentPhase = phase
  runtime.phaseStartedAtMs = Date.now()
  runtime.lastProgressAtMs = Date.now()
  runtime.stalledAtMs = null
}

function markRuntimeProgress(runtime) {
  runtime.lastProgressAtMs = Date.now()
  runtime.stalledAtMs = null
}

function throwIfImportCanceled(runtime) {
  if (runtime?.canceled) {
    const error = new Error('Import canceled by user.')
    error.name = 'AbortError'
    throw error
  }
}

async function countZipFileEntries(zipPath) {
  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (openError, zipFile) => {
      if (openError) {
        reject(openError)
        return
      }

      let fileCount = 0
      zipFile.readEntry()
      zipFile.on('entry', (entry) => {
        if (!/\/$/.test(entry.fileName)) {
          fileCount += 1
        }
        zipFile.readEntry()
      })
      zipFile.on('end', () => resolve(fileCount))
      zipFile.on('error', (error) => reject(error))
    })
  })
}

function normalizeRelativePath(value) {
  if (typeof value !== 'string' || !value.trim()) {
    return ''
  }
  return value.replaceAll('\\', '/').replace(/^\/+/, '').replace(/\/+$/, '')
}

async function getSubdirectories(dirPath) {
  const entries = await fs.readdir(dirPath, { withFileTypes: true })
  return entries.filter((entry) => entry.isDirectory()).map((entry) => path.join(dirPath, entry.name))
}

async function detectSectionsAtRoots(rootCandidates) {
  for (const candidateRoot of rootCandidates) {
    const detectedSections = await detectSections(candidateRoot)
    if (detectedSections.length > 0) {
      return { exportRootPath: candidateRoot, detectedSections }
    }
  }
  return { exportRootPath: rootCandidates[0], detectedSections: [] }
}

async function validateDiscordExportStructure(sourcePath, importSourceType) {
  const normalizedSourcePath = path.resolve(sourcePath)
  const expectedSections = KNOWN_SECTION_DETECTORS.map((item) => item.name)
  let candidateRoots = [normalizedSourcePath]

  if (importSourceType === 'zip') {
    candidateRoots = [normalizedSourcePath, ...(await getSubdirectories(normalizedSourcePath))]
  } else {
    candidateRoots = [normalizedSourcePath, ...(await getSubdirectories(normalizedSourcePath))]
  }

  const { exportRootPath, detectedSections } = await detectSectionsAtRoots(candidateRoots)
  const missingSections = expectedSections.filter((section) => !detectedSections.includes(section))
  const warnings = []

  if (detectedSections.length === 0) {
    warnings.push(
      'No known Discord data sections were detected. Confirm you selected the root of a Discord export.',
    )
  } else if (missingSections.length > 0) {
    warnings.push(
      `Partial import: some expected export sections were not found (${missingSections.join(', ')}). Available sections were parsed.`,
    )
  }

  return {
    selectedSourcePath: normalizedSourcePath,
    exportRootPath,
    detectedSections,
    missingSections,
    warnings,
  }
}

async function extractZipWithProgress(zipPath, outputDir, onProgress) {
  let filesTotal = null
  try {
    filesTotal = await countZipFileEntries(zipPath)
  } catch {
    filesTotal = null
  }

  let filesDone = 0
  await extract(zipPath, {
    dir: outputDir,
    onEntry: (entry) => {
      if (/\/$/.test(entry.fileName)) {
        return
      }
      filesDone += 1
      if (typeof onProgress === 'function') {
        onProgress({
          phase: 'extracting',
          filesTotal,
          filesDone,
          message: `Extracted ${filesDone}${filesTotal ? `/${filesTotal}` : ''} files`,
        })
      }
    },
  })
}

async function computeDirectorySha256(dirPath) {
  const hash = crypto.createHash('sha256')
  const rootPath = path.resolve(dirPath)

  async function walk(currentPath) {
    const entries = await fs.readdir(currentPath, { withFileTypes: true })
    entries.sort((a, b) => a.name.localeCompare(b.name))

    for (const entry of entries) {
      const absoluteEntryPath = path.join(currentPath, entry.name)
      const relativePath = normalizeRelativePath(path.relative(rootPath, absoluteEntryPath))
      if (entry.isDirectory()) {
        hash.update(`dir:${relativePath}\n`)
        await walk(absoluteEntryPath)
      } else if (entry.isFile()) {
        const stats = await fs.stat(absoluteEntryPath)
        hash.update(`file:${relativePath}:${stats.size}:${Math.floor(stats.mtimeMs)}\n`)
      }
    }
  }

  await walk(rootPath)
  return hash.digest('hex')
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
  const importSourceType = payload.importSourceType === 'folder' ? 'folder' : 'zip'
  const sourceLabel = importSourceType === 'folder' ? 'folder' : 'ZIP file'

  emitImportStatus(event, {
    importId,
    state: IMPORT_STATES.SELECTING_FILE,
    message: `Waiting for ${sourceLabel} selection.`,
  })

  try {
    let selectedSourcePath = typeof payload.sourcePath === 'string' && payload.sourcePath.trim() ? payload.sourcePath : null
    if (!selectedSourcePath && importSourceType === 'zip') {
      selectedSourcePath = typeof payload.zipPath === 'string' && payload.zipPath.trim() ? payload.zipPath : null
    }
    if (!selectedSourcePath) {
      const isFolderMode = importSourceType === 'folder'
      const { canceled, filePaths } = await dialog.showOpenDialog({
        title: 'Select Discord Data Package',
        properties: isFolderMode ? ['openDirectory'] : ['openFile'],
        filters: isFolderMode
          ? undefined
          : [
              { name: 'ZIP archive', extensions: ['zip'] },
              { name: 'All files', extensions: ['*'] },
            ],
      })
      if (canceled || filePaths.length === 0) {
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.CANCELED,
          importSourceType,
          sourcePath: selectedSourcePath,
          message: 'Import canceled before file selection.',
        })
        return null
      }
      selectedSourcePath = filePaths[0]
    }

    const warnings = []
    let exportRootPath = selectedSourcePath
    const importsRoot = path.join(app.getPath('userData'), IMPORTS_SUBDIR)
    const analyticsCacheRoot = path.join(app.getPath('userData'), ANALYTICS_CACHE_SUBDIR)

    await cleanupOldImports(importsRoot)
    await fs.mkdir(analyticsCacheRoot, { recursive: true })

    const rootPath = path.join(importsRoot, importId)
    const importHash =
      importSourceType === 'folder'
        ? await computeDirectorySha256(selectedSourcePath)
        : await computeFileSha256(selectedSourcePath)
    const runtime = createImportRuntime(importId, importHash, {
      importSourceType,
      selectedSourcePath,
      phaseTimeoutsMs: payload.phaseTimeoutsMs,
      stallThresholdMs: payload.stallThresholdMs,
    })
    activeParserJobs.set(importId, runtime)
    startRuntimeMonitor(event, runtime)

    try {
      const cachedAnalytics = await loadAnalyticsCache(analyticsCacheRoot, importHash)
      if (importSourceType === 'zip') {
        await fs.mkdir(rootPath, { recursive: true })
        setRuntimePhase(runtime, 'extracting')
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.EXTRACTING_ZIP,
          importSourceType,
          sourcePath: selectedSourcePath,
          message: `Extracting ZIP archive from ${selectedSourcePath}.`,
        })
        emitParserProgress(event, { importId, importHash }, {
          phase: 'extracting',
          percent: 0,
          filesDone: 0,
          message: `Starting ZIP extraction from ${selectedSourcePath}.`,
        })
        markRuntimeProgress(runtime)
        await extractZipWithProgress(selectedSourcePath, rootPath, (progress) => {
          markRuntimeProgress(runtime)
          emitParserProgress(event, { importId, importHash }, progress)
        })
        throwIfImportCanceled(runtime)
        exportRootPath = rootPath
      }

      setRuntimePhase(runtime, 'scanning')
      emitImportStatus(event, {
        importId,
        state: IMPORT_STATES.SCANNING_FILES,
        importSourceType,
        sourcePath: selectedSourcePath,
        message: `Scanning ${importSourceType} source for Discord sections: ${selectedSourcePath}`,
      })
      emitParserProgress(event, { importId, importHash }, {
        phase: 'scanning',
        message: `Scanning ${importSourceType} source for known sections.`,
      })
      markRuntimeProgress(runtime)
      const preflight = await validateDiscordExportStructure(exportRootPath, importSourceType)
      throwIfImportCanceled(runtime)
      const detectedSections = preflight.detectedSections
      const missingSections = preflight.missingSections
      warnings.push(...preflight.warnings)
      exportRootPath = preflight.exportRootPath

      let parsedExport
      if (cachedAnalytics) {
        setRuntimePhase(runtime, 'aggregating')
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.AGGREGATING,
          importSourceType,
          sourcePath: selectedSourcePath,
          message: `Using cached analytics for ${importSourceType} source.`,
        })
        parsedExport = cachedAnalytics
        markRuntimeProgress(runtime)
        emitParserProgress(event, { importId, importHash }, {
          phase: 'complete',
          percent: 100,
          recordsDone: parsedExport.analyticsSummary?.messageCount ?? 0,
          message: 'Cache hit: loaded analytics from previous import.',
        })
      } else {
        setRuntimePhase(runtime, 'parsing')
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.PARSING,
          importSourceType,
          sourcePath: selectedSourcePath,
          message: `Parsing Discord export files from ${exportRootPath}.`,
        })
        const parseAbortController = new AbortController()
        runtime.parseAbortController = parseAbortController
        let latestProgress = null
        let heartbeatInterval = null
        try {
          heartbeatInterval = setInterval(() => {
            if (!latestProgress) {
              return
            }

            markRuntimeProgress(runtime)
            emitParserProgress(event, { importId, importHash }, {
              ...latestProgress,
              heartbeat: true,
              message: `${latestProgress.message} (still working)`,
            })
          }, 1500)

          parsedExport = await parseDiscordExport(exportRootPath, {
            sortDirection: 'asc',
            signal: parseAbortController.signal,
            onProgress: (progress) => {
              latestProgress = progress
              markRuntimeProgress(runtime)
              emitParserProgress(event, { importId, importHash }, progress)
            },
          })
          throwIfImportCanceled(runtime)
          setRuntimePhase(runtime, 'aggregating')
          emitImportStatus(event, {
            importId,
            state: IMPORT_STATES.AGGREGATING,
            importSourceType,
            sourcePath: selectedSourcePath,
            message: 'Aggregating analytics output.',
          })
          markRuntimeProgress(runtime)
          await persistAnalyticsCache(analyticsCacheRoot, importHash, parsedExport)
        } finally {
          if (heartbeatInterval) {
            clearInterval(heartbeatInterval)
          }
        }
      }

      await appendParserDebugLog(importId, [...warnings, ...parsedExport.warnings])
      parsedArchiveCache.set(importId, parsedExport)
      setRuntimePhase(runtime, 'complete')

      emitImportStatus(event, {
        importId,
        state: IMPORT_STATES.COMPLETED,
        importSourceType,
        sourcePath: selectedSourcePath,
        message: 'Import completed successfully.',
      })
      return {
        ok: true,
        importId,
        importHash,
        cacheHit: Boolean(cachedAnalytics),
        importSourceType,
        sourcePath: selectedSourcePath,
        rootPath: exportRootPath,
        selectedZipPath: importSourceType === 'zip' ? selectedSourcePath : null,
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
      if (error?.name === 'AbortError') {
        await fs.rm(rootPath, { recursive: true, force: true })
        emitImportStatus(event, {
          importId,
          state: IMPORT_STATES.CANCELED,
          importSourceType,
          sourcePath: selectedSourcePath,
          message: 'Import canceled by user.',
        })
        return {
          ok: false,
          canceled: true,
          importId,
          importHash,
          importSourceType,
          sourcePath: selectedSourcePath,
          rootPath: exportRootPath,
          selectedZipPath: importSourceType === 'zip' ? selectedSourcePath : null,
          stallDiagnostics: activeParserJobs.get(importId)?.diagnostics || [],
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
      stopRuntimeMonitor(activeParserJobs.get(importId))
      activeParserJobs.delete(importId)
    }
  } catch (error) {
    emitImportStatus(event, {
      importId,
      state: IMPORT_STATES.FAILED,
      importSourceType,
      sourcePath: typeof payload.sourcePath === 'string' ? payload.sourcePath : null,
      message: `Import failed: ${error.message}`,
    })
    return {
      ok: false,
      importId,
      importSourceType,
      sourcePath: typeof payload.sourcePath === 'string' ? payload.sourcePath : null,
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

  const runtime = activeParserJobs.get(importId)
  runtime.canceled = true
  runtime.parseAbortController?.abort()
  recordDiagnostic(runtime, {
    type: 'cancel-requested',
    phase: runtime.currentPhase,
    elapsedMs: Date.now() - runtime.phaseStartedAtMs,
  })
  return { ok: true, canceled: true, phase: runtime.currentPhase }
}

function handleGetImportDiagnostics(_event, payload = {}) {
  const importId = payload.importId
  if (!importId || !activeParserJobs.has(importId)) {
    return { ok: false, diagnostics: [] }
  }

  const runtime = activeParserJobs.get(importId)
  return {
    ok: true,
    importId,
    phase: runtime.currentPhase,
    elapsedMs: Date.now() - runtime.phaseStartedAtMs,
    stallCount: runtime.stallCount,
    diagnostics: runtime.diagnostics,
    phaseTimeoutsMs: runtime.phaseTimeoutsMs,
    stallThresholdMs: runtime.stallThresholdMs,
  }
}

app.whenReady().then(() => {
  ipcMain.handle('dialog:pick-data-package', handleSelectZip)
  ipcMain.handle('dialog:select-import-source', handleSelectZip)
  ipcMain.handle('dialog:select-zip', handleSelectZip)
  ipcMain.handle('parser:get-analytics', handleGetParserAnalytics)
  ipcMain.handle('parser:cancel-job', handleCancelParserJob)
  ipcMain.handle('import:get-diagnostics', handleGetImportDiagnostics)

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
