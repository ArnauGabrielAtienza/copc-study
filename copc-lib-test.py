"""Some tests using copc-lib (cat_tst.copc.laz) to use
    COPC Lidar files."""
import copclib as copc
import os

###Read File
DATA = "autzen-classified.copc.laz"
reader = copc.FileReader(DATA)

###Read header
def read_header():
    header = reader.copc_config.las_header
    print(header)


###Read some info regarding COPC 
def read_copc_info():
    copc_info = reader.copc_config.copc_info
    print(copc_info)
    print("\tRoot Offset: ", copc_info.root_hier_offset)
    print("\tRoot Size: ", copc_info.root_hier_size)
    
    copc_extents = reader.copc_config.copc_extents
    print("CopcExtents(min,max):")
    print(
        "\tIntensity: (%f,%f):"
        % (copc_extents.intensity.minimum, copc_extents.intensity.maximum)
    )
    print(
        "\tClassification: (%f,%f)"
        % (copc_extents.classification.minimum, copc_extents.classification.maximum)
    )
    print(
        "\tUser Data: (%f,%f)"
        % (copc_extents.user_data.minimum, copc_extents.user_data.maximum)
    )
   
    
###Find a node and its points
def find_node():
    load_key = (4, 11, 9, 0)                #Key related to the node (depth, x, y, z)
    node = reader.FindNode(load_key)        #Find the node
    node_points = reader.GetPoints(node)    #Get the points inside an array
    print("\n", node)
    print("\n", node_points)
    print("\nFirst 3 points:")
    for point in node_points[:3]:
        print(point)
        

###Find the points given 2D coordinates 
def spatial_query():
    #Define Area
    middle = (reader.copc_config.las_header.max + reader.copc_config.las_header.min) / 2
    middle_box = (middle.x - 200, middle.y - 200, middle.x + 200, middle.y + 200)
    
    #Get Nodes
    nodes = reader.GetNodesIntersectBox(middle_box)
    print("Number of nodes: ", len(nodes))
    
    #Get Points
    points = reader.GetPointsWithinBox(middle_box)
    print("Number of points: ", len(points))
    
    #Print Some Points
    print("\nFirst 3 points:")
    for point in points[:3]:
        print(point)
    
    

if __name__ == "__main__":
    read_header()
    read_copc_info()
    find_node()
    spatial_query()
    print(dir(copc))
    reader.Close()