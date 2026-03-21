"""
Logger configuration for middlend - re-exports from middleware.
"""
import os
from middleware.utils.loggerConfig import setupLogging

def getProjectDir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def setupLoggingMiddlend():
    return setupLogging(logPara="middlend", projectDir=getProjectDir())
