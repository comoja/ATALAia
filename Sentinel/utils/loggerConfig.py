"""
Logger configuration for Sentinel - re-exports from middleware.
"""
import os
from middleware.utils.loggerConfig import setupLogging

def getProjectDir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def setupLoggingSentinel(enableConsole: bool = True):
    return setupLogging(logPara="sentinel", projectDir=getProjectDir(), enableConsole=enableConsole)
