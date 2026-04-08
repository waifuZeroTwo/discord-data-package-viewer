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
const PARSER_DEBUG_LOG = 'parser-debug.log'
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

  const parsedExport = await parseDiscordExport(rootPath, { sortDirection: 'asc' })
  await appendParserDebugLog(importId, [...warnings, ...parsedExport.warnings])
  parsedArchiveCache.set(importId, parsedExport)

  const messageCount = Object.values(parsedExport.messagesByChannel).reduce(
    (count, messages) => count + messages.length,
    0,
  )

  return {
    ok: true,
    importId,
    rootPath,
    warnings: [...warnings, ...parsedExport.warnings],
    detectedSections,
    missingSections,
    parserSummary: {
      channelCount: parsedExport.channels.length,
      messageCount,
      sortDirection: parsedExport.sortDirection,
    },
  }
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

async function handleGetParserPage(_event, payload = {}) {
  const importId = payload.importId
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

  if (payload.sortDirection && payload.sortDirection !== parsedExport.sortDirection) {
    const sortedExport = {
      ...parsedExport,
      messagesByChannel: { ...parsedExport.messagesByChannel },
      sortDirection: payload.sortDirection === 'desc' ? 'desc' : 'asc',
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
    ...getPaginatedParserOutput(currentParse, payload),
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
