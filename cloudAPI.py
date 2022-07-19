import laspy.copc as lasp
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple

class CopcHelper:
    """
    The CopcHelper class provides some functionalities to help reading a COPC file stored
    in the cloud using the laspy library. We take advantage of range downloads to avoid
    downloading the whole file.
    
    Args:
        bucket (str): Storage bucket where the file is stored
        key (str): File key inside the bucket
        filename (str): Name that will be given to the local file
        s3: boto3 instance with access to the Object Storage
    """
    def __init__(self, bucket: str, key: str, filename: str, s3):
        self.bucket = bucket
        self.key = key
        self.filename = filename
        self.ibm_cos = s3
        
    def load_header(self):
        """Download the file header and store it in a local file. This function should be one of the first
        to be called.
        """
        #TODO Change the range to the exact one. It is currently an aproximation.
        header_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(0, 1024))
        body = header_request['Body']
        f = open(self.filename, "w+b")
        for i in body:
            f.write(i)
        f.close()

    def rootpage_location(self) -> Tuple[int, int]:
        """Get the location and size of the octree list. The header needs to be loaded first.

        Returns:
            Tuple[int, int]: Root Offset and Root Size
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        root_offset = reader.copc_info.hierarchy_root_offset
        root_size = reader.copc_info.hierarchy_root_size
        f.close()
        return root_offset, root_size

    def load_rootpage(self):
        """Download the rootpage/octree and store it in the local file.
        """
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
     
    def get_point_count(self) -> int:
        """Returns the total amount of points in the file. Only the header needs to be loaded.

        Returns:
            int: Number of points
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        points = reader.header.point_count
        f.close()
        return points
   
    def get_all_keys(self) -> list[lasp.VoxelKey]:
        """Get the keys of all existing nodes in the file.

        Returns:
            list[lasp.VoxelKey]: List of all keys
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        keys = reader.root_page.entries.keys()
        f.close()
        return keys
    
    def get_all_nodes(self) -> list[lasp.Entry]:
        """Get all the existing nodes (only their metadata).

        Returns:
            list[lasp.Entry]: List of all nodes
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        keys = reader.root_page.entries.keys()
        f.close()
        nodes = []
        for key in keys:
            nodes.append(reader.root_page.entries.get(key))
        return nodes

    def get_level_nodes(self, level: int) -> list[lasp.Entry]:
        """Get all the existing nodes in a given level (only their metadata).

        Args:
            level (int): Depth level

        Returns:
            list[lasp.Entry]: List of existing nodes in level
        """
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
    
    def get_level_keys(self, level: int) -> list[lasp.VoxelKey]:
        """Get all the existing node keys in a given level.

        Args:
            level (int): Depth level

        Returns:
            list[lasp.Entry]: List of existing node keys in level
        """
        #Get the keys of the nodes in that level
        keys = self.get_all_keys()
        node_keys = []
        for key in keys:
            if(key.level==level):
                node_keys.append(key)
        return node_keys
    
    def get_node(self, key: lasp.VoxelKey) -> lasp.Entry:
        """Get the metadata of a node given its key

        Args:
            key (lasp.VoxelKey): Node key

        Returns:
            lasp.Entry: Node information
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        node = reader.root_page.entries.get(key)
        f.close()
        return node
     
    def get_parent(self, key: lasp.VoxelKey) -> lasp.VoxelKey:
        """Get the theoretical parent key, even though it may not exist in the octree.

        Args:
            key (lasp.VoxelKey): Child key

        Returns:
            lasp.VoxelKey: Parent key
        """
        parent = lasp.VoxelKey()
        parent.x = int((key.x)/2)
        parent.y = int((key.y)/2)
        parent.level = key.level - 1
        return parent
    
    def get_children(self, parentkey: lasp.VoxelKey) -> list[lasp.VoxelKey]:
        """Get all existing children of a node given its key.

        Args:
            parentkey (lasp.VoxelKey): Parent key

        Returns:
            list[lasp.VoxelKey]: List of child keys
        """
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

    def multiple_points_download(self, nodes: list[lasp.Entry]):
        """Download and load into file multiple nodes (their points) at the same time using threads.

        Args:
            nodes (list[lasp.Entry]): Nodes to be loaded
        """
        with ThreadPoolExecutor() as executor:
            executor.map(self.load_points, nodes)
    
    def load_points(self, node: lasp.Entry):
        """Download a set of points in a node and load the into the local file.

        Args:
            node (lasp.Entry): Node to be downloaded
        """
        #Download the node
        point_request = self.ibm_cos.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(node.offset, node.offset + node.byte_size))
        body = point_request['Body']
        
        #Load the node into the file
        f = open(self.filename, "r+b")
        f.seek(node.offset)
        for i in body:
            f.write(i)
        f.close()
      
    def get_points(self, nodes: list[lasp.Entry]) -> lasp.ScaleAwarePointRecord:
        """Retrieve the points of a list of nodes. The should be downloaded first 
        using multiple_points_download() or load_points().

        Args:
            nodes (list[lasp.Entry]): Nodes to be retrieved

        Returns:
            lasp.ScaleAwarePointRecord: List of points
        """
        f = open(self.filename, "r+b")
        reader = lasp.CopcReader(f)
        points = reader._fetch_and_decrompress_points_of_nodes(nodes)
        f.close()
        return points
    
    def point_balance(self, points: int) -> list[list[lasp.Entry]]:
        """Split the nodes into different groups. Each group will have a similar amount of points.
        This allows better balance if we want to split the file between different workers.

        Args:
            points (int): Amount of points per worker

        Returns:
            list[list[lasp.Entry]]: List with the node list that each worker should compute
        """
        balance = []
        
        keys = self.get_all_nodes()
        
        worker = []
        num_points = 0
        for node in keys:
            num_points += node.point_count
            worker.append(node)
            if num_points > points:
                num_points = 0
                balance.append(worker)
                worker = []
                
        if num_points > 0:
            balance.append(worker)
        
        return balance
    
    def size_balance(self, size: int) -> list[list[lasp.Entry]]:
        """Split the nodes into different groups. Each group will have a similar download size.
        This allows better balance if we want to split the file between different workers.

        Args:
            size (int): Download size per worker

        Returns:
            list[list[lasp.Entry]]: List with the node list that each worker should compute
        """
        balance = []
        
        keys = self.get_all_nodes()
        
        worker = []
        num_size = 0
        for node in keys:
            num_size += node.byte_size
            worker.append(node)
            if num_size > size:
                num_size = 0
                balance.append(worker)
                worker = []
                
        if num_size > 0:
            balance.append(worker)
        
        return balance