module.exports = {
    apps : [
        {
            name: "Sentinel-App",
            script: "/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/Sentinel/main.py",
            interpreter: "python3",
            cwd: "/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/Sentinel/", // Directorio de trabajo
            watch: false
        },
        {
            name: "dataSymbol-App",
            script: "/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/dataSymbol/mainOrchestrator.py",
            interpreter: "python3",
            cwd: "/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/dataSymbol/", // Directorio de trabajo
            watch: false
        }
    ]
  };