module.exports = {
    apps: [
        {
            name: "Sentinel-App",
            script: "/Volumes/TimeMachine/ATALAia/Sentinel/main.py",
            interpreter: "python3",
            cwd: "/Volumes/TimeMachine/ATALAia/Sentinel/", // Directorio de trabajo
            watch: false
        },
        {
            name: "dataSymbol-App",
            script: "/Volumes/TimeMachine/ATALAia/dataSymbol/mainOrchestrator.py",
            interpreter: "python3",
            cwd: "/Volumes/TimeMachine/ATALAia/dataSymbol/", // Directorio de trabajo
            watch: false
        }
    ]
};