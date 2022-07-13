import lithops
import laspy.copc as lasp
from cloudAPI import CopcHelper

#Lithops Function
def get_nodes(bucket, key, filename, ibm_cos):
    helper = CopcHelper(bucket, key, filename, ibm_cos)
    helper.load_header()
    helper.load_rootpage()
    
    newkey = lasp.VoxelKey()
    newkey.x = 1
    newkey.y = 2
    newkey.z = 0
    newkey.level = 2

    #return helper.isin(list(helper.get_all_keys()), newkey)
    #return helper.get_smallest_neighbours(newkey)
    #return list(helper.get_all_keys())
    #return helper.get_level_nodes(4)
    return helper.get_children(newkey)


if __name__ == "__main__":
    args = {'bucket': 'lithops-testing3', 'key': 'autzen-classified.copc.laz', 'filename': 'lidar.copc.laz'}
    fexec = lithops.LocalhostExecutor()
    fexec.call_async(get_nodes, args)
    print(fexec.get_result())