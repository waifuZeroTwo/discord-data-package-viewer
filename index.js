const path = require('path')
const fs = require('fs/promises')
const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const extract = require('extract-zip')

const IMPORTS_SUBDIR = 'imports'
const IMPORT_RETENTION_MS = 1000 * 60 * 60 * 24 * 7
const IMPORT_MAX_COUNT = 10

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
  }

  const recentImports = importDirs.filter((item) => !item.isExpired)
  if (recentImports.length <= IMPORT_MAX_COUNT) {
    return
  }

  recentImports.sort((a, b) => a.mtimeMs - b.mtimeMs)
  const dirsToDelete = recentImports.slice(0, recentImports.length - IMPORT_MAX_COUNT)
  for (const importDir of dirsToDelete) {
    await fs.rm(importDir.fullPath, { recursive: true, force: true })
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
    }
  }

  const detectedSections = await detectSections(rootPath)

  if (detectedSections.length === 0) {
    warnings.push(
      'No known Discord data sections were detected (expected directories like messages or account).',
    )
  }

  return {
    ok: detectedSections.length > 0,
    importId,
    rootPath,
    warnings,
    detectedSections,
  }
}

app.whenReady().then(() => {
  ipcMain.handle('dialog:pick-data-package', handleSelectZip)
  ipcMain.handle('dialog:select-zip', handleSelectZip)

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
