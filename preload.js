const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('ddpApi', {
  pickDataPackage: () => ipcRenderer.invoke('dialog:pick-data-package'),
  selectZip: () => ipcRenderer.invoke('dialog:select-zip'),
  getParserAnalytics: (options) => ipcRenderer.invoke('parser:get-analytics', options),
  getRuntimeVersions: () => ({
    electron: process.versions.electron,
    chrome: process.versions.chrome,
    node: process.versions.node,
  }),
})
