"""Some test using COPC Lidar files on the Cloud. We have
    used Lithops middleware while having the file on the IBM
    Cloud. The Objective was to access all the metadata needed
    to use a COPC file using HTTP GET. This way we only have
    to download a really small section of the file (<1MB) while
    the original file was ~70MB, improving times a lot."""
import lithops
import laspy.copc as lasp
import laspy

#Args given to the Lithops Function Executor
args = {'bucket': 'lithops-testing3', 'key': 'cat_tst.copc.laz', 'filename': 'lidar.copc.laz'}

#CopcHelper helps in the process of reading a COPC file using laspy.
#The files are stored in the cloud and we take advantage of range downloads.
class CopcHelper:
    def __init__(self, bucket, key, filename, s3):
        self.bucket = bucket
        self.key = key
        self.filename = filename
        self.ibm_cos = s3
        self.load_header()
        self.load_rootpage()
        
    #Download file header and save it into a file.
    #This function should be one of the first to
    #be called.
    def load_header(self):
        #TODO Change the range to the exact one. It is currently an aproximation.
        headerreq = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(0, 1024))
        body = headerreq['Body']
        f = open(self.filename, "w+b")
        for i in body:
            f.write(i)
        f.close()

    #Read the location and size of the rootpage.
    def rootpage_location(self):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        root_offset = reader.copc_info.hierarchy_root_offset
        root_size = reader.copc_info.hierarchy_root_size
        f.close()
        return root_offset, root_size

    #Load the rootpage into the main file.
    #Needed in order to read the keys of the octree
    #and the information of all nodes.    
    def load_rootpage(self):
        rootpage = self.rootpage_location()
        root_offset = rootpage[0]
        root_size = rootpage[1]
        
        #Download rootpage
        rootpage_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(root_offset, root_offset + root_size))
        body = rootpage_request['Body']
        
        #Load rootpage into file
        f = open(self.filename, "r+b")
        f.seek(root_offset)
        for i in body:
            f.write(i)
        f.close()

    #Get all available keys.    
    def get_all_keys(self):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        keys = reader.root_page.entries.keys()
        f.close()
        return keys

    #Get the information of all nodes
    #in a given level
    def get_level_nodes(self, level):
        #Get the keys of the nodes in the level
        keys = self.get_all_keys()
        node_keys = []
        for key in keys:
            if(key.level==level):
                node_keys.append(key)
                
        #Get the node info
        result = []
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        for key in node_keys:
            result.append(str(reader.root_page.entries.get(key)))
        f.close()
        return result
    
    #Get info of a single node
    def get_node(self, key):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        node = reader.root_page.entries.get(key)
        f.close()
        return node
    
    #Get the upper level parent of a node    
    def get_parent(self, key):
        parent = lasp.VoxelKey()
        parent.x = int((key.x)/2)
        parent.y = int((key.y)/2)
        parent.level = key.level - 1
        return parent
    
    #Get all children of a given node.
    #TODO Remove children that do not exist in the
    #real file, since some theorical nodes may not
    #exist since they would be empty.
    def get_children(self, parentkey):
        keys = []
        x = parentkey.x
        y = parentkey.y
        z = parentkey.z
        level = parentkey.level + 1
        
        for i in range(z, z+2):
            key = lasp.VoxelKey()
            key.x = x*2
            key.y = y*2
            key.z = i
            key.level = level
            keys.append(key)
            
            key = lasp.VoxelKey()
            key.x = x*2 + 1
            key.y = y*2
            key.z = i
            key.level = level
            keys.append(key)

            key = lasp.VoxelKey()
            key.x = x*2
            key.y = y*2 + 1
            key.z = i
            key.level = level
            keys.append(key)
            
            key = lasp.VoxelKey()
            key.x = x*2 + 1
            key.y = y*2 + 1
            key.z = i
            key.level = level
            keys.append(key)
            
        return keys
    
    #Load points into memory
    def load_points(self, key):
        node = self.get_node(key)
        headerreq = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(node.offset, node.offset + node.byte_size))
        body = headerreq['Body']
        f = open(self.filename, "r+b")
        f.seek(node.offset)
        for i in body:
            f.write(i)
        f.close()
    
    #Retrieve points from a given key. It should be loaded
    #into memory first.    
    def get_points(self, key):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        points = reader._fetch_and_decrompress_points_of_nodes([reader.root_page.entries.get(key)])
        f.close()
        return points
        
        

#Lithops Function
def get_nodes(bucket, key, filename, ibm_cos):
    helper = CopcHelper(bucket, key, filename, ibm_cos)
    
    var = lasp.VoxelKey()
    var.x = 13
    var.y = 15
    var.z = 5
    var.level = 4
    helper.load_points(var)
    points = helper.get_points(var)

    return points.array[0]


if __name__ == "__main__":
    fexec = lithops.LocalhostExecutor()
    fexec.call_async(get_nodes, args)
    print(fexec.get_result())