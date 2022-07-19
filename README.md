# copc-testing
A small repo with code I have developed while studying the COPC Lidar File Format. 

cloudAPI.py contains the CopcHelper class, which provides some functionalities to help reading a COPC file stored
in the cloud using the laspy library. We take advantage of range downloads to avoid downloading the whole file.
All those downloads are stored in a local file to avoid repeating the same download.