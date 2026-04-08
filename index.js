const path = require('path')
const { app, BrowserWindow, ipcMain, dialog } = require('electron')

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

app.whenReady().then(() => {
  ipcMain.handle('dialog:pick-data-package', async () => {
    const { canceled, filePaths } = await dialog.showOpenDialog({
      title: 'Select Discord Data Package',
      properties: ['openFile'],
      filters: [
        { name: 'Supported archive', extensions: ['zip'] },
        { name: 'All files', extensions: ['*'] },
      ],
    })

    if (canceled || filePaths.length === 0) {
      return null
    }

    return filePaths[0]
  })

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
