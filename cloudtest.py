"""Some test using COPC Lidar files on the Cloud. We have
    used Lithops middleware while having the file on the IBM
    Cloud. The Objective was to access all the metadata needed
    to use a COPC file using HTTP GET. This way we only have
    to download a really small section of the file (<1MB) while
    the original file was ~70MB, improving times a lot."""
import lithops
import os
import copclib as copc
import io
import laspy.copc as lasp

FILE = "cat_tst.copc.laz"

def get_nodes(num, ibm_cos):
    #Download Header and Save to File
    headerreq = ibm_cos.get_object(Bucket='lithops-testing2', Key=FILE, Range='bytes={}-{}'.format(0, 1000))
    body = headerreq['Body']
    f = open("lidar.copc.laz", "w+b")
    for i in body:
        f.write(i)
    f.close()
    
    #Get the root page offset
    f = open("lidar.copc.laz", "r+b")
    reader = lasp.CopcReader(f)
    root_offset = reader.copc_info.hierarchy_root_offset
    root_size = reader.copc_info.hierarchy_root_size
    f.close()
    
    #Download Root Page and Save to File    
    firstpage = ibm_cos.get_object(Bucket='lithops-testing2', Key=FILE, Range='bytes={}-{}'.format(root_offset, root_offset + root_size))
    body2 = firstpage['Body']
    
    f = open("lidar.copc.laz", "r+b")
    f.seek(root_offset)
    for i in body2:
        f.write(i)
    f.close()
    
    #Get All Keys
    key = lasp.VoxelKey()
    key.level = 2
    key.x = 0
    key.y = 0
    key.z = 0
    f = open("lidar.copc.laz", "r+b")
    reader = lasp.CopcReader(f)
    keys = reader.root_page.entries.keys()
    
    #Get info in a given level
    level = 1
    key_level = []
    for key in keys:
        if(key.level==2):
            key_level.append(key)
    
    result = []
    for key in key_level:
        result.append(str(reader.root_page.entries.get(key)))
    f.close()

    return (1000+root_size)/(1024)


if __name__ == "__main__":
    fexec = lithops.LocalhostExecutor()
    fexec.call_async(get_nodes, 0)
    print(fexec.get_result())