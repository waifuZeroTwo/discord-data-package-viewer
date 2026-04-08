const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('ddpApi', {
  pickDataPackage: () => ipcRenderer.invoke('dialog:pick-data-package'),
  selectZip: () => ipcRenderer.invoke('dialog:select-zip'),
  getParserPage: (options) => ipcRenderer.invoke('parser:get-page', options),
  getRuntimeVersions: () => ({
    electron: process.versions.electron,
    chrome: process.versions.chrome,
    node: process.versions.node,
  }),
})
