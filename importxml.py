import os
from includes.census import *

def main():     
    census = Census(dbconfig="/home/weikai/database.yaml", year=1940)

    datapath = 'data/XML'
    # parse xml file 
    file=None
    #census.parseXML('data/XML/AK.xml')  
    #census.parseXML('data/XML/AL.xml')
    #census.parseXML('data/XML/NE.xml')
    #file = 'data/XML/NE.xml'
    #file = 'data/XML/AK.xml'
    if file:
        census.parseXML(file)
    else:
        for file in os.listdir(datapath):        
            if file.endswith(".xml"):
                print(f"Processing {os.path.join(datapath, file)}")
                census.parseXML(os.path.join(datapath, file))
    
if __name__ == "__main__": 
  
    # calling main function 
    main() 
    

