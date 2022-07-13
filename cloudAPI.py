import laspy.copc as lasp

#CopcHelper helps in the process of reading a COPC file using laspy.
#The files are stored in the cloud and we take advantage of range downloads.
#Params:
#   bucket:     name of the bucket where the file is stored
#   key:        file key in the object storage
#   filename:   file name for the file that will  be stored locally
#   s3:         boto3 with access to the cloud where the file is located
class CopcHelper:
    #Initialize Reader.
    def __init__(self, bucket, key, filename, s3):
        self.bucket = bucket
        self.key = key
        self.filename = filename
        self.ibm_cos = s3
        
    #Download file header and save it into a file. This function should be one of the first to be called.
    def load_header(self):
        #TODO Change the range to the exact one. It is currently an aproximation.
        header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(0, 1024))
        body = header_request['Body']
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

    #Load the rootpage into the main file to be able to read the keys from the octree
    #and their node entries. 
    def load_rootpage(self):
        rootpage = self.rootpage_location()
        root_offset = rootpage[0]
        root_size = rootpage[1]
        
        #Download rootpage.
        rootpage_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(root_offset, root_offset + root_size))
        body = rootpage_request['Body']
        
        #Load rootpage into the file
        f = open(self.filename, "r+b")
        f.seek(root_offset)
        for i in body:
            f.write(i)
        f.close()

    #Get all existing keys.    
    def get_all_keys(self):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        keys = reader.root_page.entries.keys()
        f.close()
        return keys

    #Get the information of all nodes in a given level
    def get_level_nodes(self, level):
        #Get the keys of the nodes in that level
        keys = self.get_all_keys()
        node_keys = []
        for key in keys:
            if(key.level==level):
                node_keys.append(key)
                
        #Get the node information of the previous keys
        nodes = []
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        for key in node_keys:
            nodes.append(reader.root_page.entries.get(key))
        f.close()
        return nodes
    
    #Get information of a single node
    def get_node(self, key):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        node = reader.root_page.entries.get(key)
        f.close()
        return node
    
    #Get the parent of a node given its key   
    def get_parent(self, key):
        parent = lasp.VoxelKey()
        parent.x = int((key.x)/2)
        parent.y = int((key.y)/2)
        parent.level = key.level - 1
        return parent
    
    #Get all children of a given node.
    def get_children(self, parentkey):
        keys = []
        
        x = parentkey.x
        y = parentkey.y
        z = parentkey.z
        level = parentkey.level + 1
        
        all_existing_keys = list(self.get_all_keys())
        
        #A node can have up to 8 children.
        #TODO: Implement loops for cleaner code
        for i in range(z, z+2):
            key = lasp.VoxelKey()
            key.x = x*2
            key.y = y*2
            key.z = i
            key.level = level
            if key in all_existing_keys:
                keys.append(key)
            
            key = lasp.VoxelKey()
            key.x = x*2 + 1
            key.y = y*2
            key.z = i
            key.level = level
            if key in all_existing_keys:
                keys.append(key)

            key = lasp.VoxelKey()
            key.x = x*2
            key.y = y*2 + 1
            key.z = i
            key.level = level
            if key in all_existing_keys:
                keys.append(key)
            
            key = lasp.VoxelKey()
            key.x = x*2 + 1
            key.y = y*2 + 1
            key.z = i
            key.level = level
            if key in all_existing_keys:
                keys.append(key)
            
        return keys
    
    #Download a set of points in a node given its key. The load them into the local file
    #to be able to get them
    def load_points(self, key):
        #Download the node
        node = self.get_node(key)
        point_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(node.offset, node.offset + node.byte_size))
        body = point_request['Body']
        
        #Load the node into the file
        f = open(self.filename, "r+b")
        f.seek(node.offset)
        for i in body:
            f.write(i)
        f.close()
    
    #Retrieve points from a node given its key. The node should be loaded into memory first.    
    def get_points(self, key):
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        points = reader._fetch_and_decrompress_points_of_nodes([reader.root_page.entries.get(key)])
        f.close()
        return points
    
    #Get the neighbours of a given node at the deepest level (4).
    def get_smallest_neighbours(self, key):
        neighbours = []
        
        all_keys = list(self.get_all_keys())
        x = key.x
        y = key.y
        z = key.z
        level = key.level
        
        num = 2 ** (4-level)    #Number of level 4 nodes in our node / 2. Helpful in a lot of calculations.
        
        #In this first part we search for the nodes at the sides and their up and down version.
        #These nodes are the ones that are adjacent but are not immediately up or down of our node.
        for k in range(num+2):
            #Nodes at the back
            if(y!=-1):
                for i in range(num + 1):
                    nkey = lasp.VoxelKey()
                    nkey.level = 4
                    nkey.y = y*num - 1
                    nkey.z = z + k - 1
                    nkey.x = x*num + i
                    if nkey in all_keys:
                        neighbours.append(nkey)
                
            #Nodes at the left
            if(x!=-1):
                for i in range(num + 1):
                    nkey = lasp.VoxelKey()
                    nkey.level = 4
                    nkey.y = y*num + i - 1
                    nkey.z = z + k - 1
                    nkey.x = x*num - 1
                    if nkey in all_keys:
                        neighbours.append(nkey)
                    
            #Nodes at the front
            max = (2 ** level) - 1
            if(y!=max):
                for i in range(num + 1):
                    nkey = lasp.VoxelKey()
                    nkey.level = 4
                    nkey.y = y*num + num
                    nkey.z = z + k - 1
                    nkey.x = x*num + i - 1
                    if nkey in all_keys:
                        neighbours.append(nkey)
                    
            #Nodes at the right
            if(x!=max):
                for i in range(num + 1):
                    nkey = lasp.VoxelKey()
                    nkey.level = 4
                    nkey.y = y*num + i
                    nkey.z = z + k - 1
                    nkey.x = x*num + num
                    if nkey in all_keys:
                        neighbours.append(nkey)
        
        #Now we need to find the nodes above and below our node. In the case that we are already at level 4
        #we just need the node above/below us. To do this we calculate the height that they should have and
        #then we 
        #Up
        if level==4:
            upkey = lasp.VoxelKey()
            upkey.x = x
            upkey.y = y
            upkey.z = z+1
            upkey.level = 4
            if nkey in all_keys:
                neighbours.append(nkey)
        else:
            for i in range(num):
                for j in range(num):
                    nkey = lasp.VoxelKey()
                    nkey.x = x + i
                    nkey.y = y + j
                    nkey.z = z+num
                    nkey.level = 4
                    if nkey in all_keys:
                        neighbours.append(nkey)
        
        #Down
        if level==4:
            upkey = lasp.VoxelKey()
            upkey.x = x
            upkey.y = y
            upkey.z = z-1
            upkey.level = 4
            if nkey in all_keys:
                neighbours.append(nkey)
        else:
            for i in range(num):
                for j in range(num):
                    nkey = lasp.VoxelKey()
                    nkey.x = x + i
                    nkey.y = y + j
                    nkey.z = z*num - 1
                    nkey.level = 4
                    if nkey in all_keys:
                        neighbours.append(nkey)
                        
        return neighbours