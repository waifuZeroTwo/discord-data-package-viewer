const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('ddpApi', {
  selectZip: () => ipcRenderer.invoke('dialog:select-zip'),
  getParserPage: (options = {}) => ipcRenderer.invoke('parser:get-page', options),
  getRuntimeVersions: () => ({
    electron: process.versions.electron,
    chrome: process.versions.chrome,
  }),
})
