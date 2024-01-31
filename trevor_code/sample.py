from ctypes import *
import platform
thePlatform = platform.system() 
if (thePlatform =="Windows"):
    _proj=CDLL(r'./Windows/sample.dll')
elif (thePlatform =="Darwin"):
    _proj=CDLL(r'./Mac/sample.dll')
elif (thePlatform=="Linux"):
    _proj=CDLL(r'./Linux/sample.dll')
else :
    raise Exception("Operating system not currently supported")
_proj . function ()