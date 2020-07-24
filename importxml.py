from os import listdir
from includes.census import *

def list_files(directory, extension):
    return (f for f in listdir(directory) if f.endswith('.' + extension))



def main():     
    census = Census(year=1940)

    datapath = 'data/XML'
    # parse xml file 
    file=None
    #census.parseXML('data/XML/AK.xml')  
    #census.parseXML('data/XML/AL.xml')
    #census.parseXML('data/XML/NE.xml')
    #file = 'data/XML/NE.xml'
    file = 'data/XML/AK.xml'
    if file:
        census.parseXML(file)
    else:
        files = list_files(datapath, "xml")
        total=len(files)
        c=1        
        for file in files:                                
            print(f"{c}/{total}: Processing {os.path.join(datapath, file)}")
            census.parseXML(os.path.join(datapath, file))
    
if __name__ == "__main__": 
  
    # calling main function 
    main() 
    

