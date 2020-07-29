import os,sys
from includes.census import *

def list_files(directory, extension):
    
    files=[]
    for f in os.listdir(directory):
        if f.endswith('.' + extension):
            files.append(f)

    files.sort()
    return files


def main(file=''):     
    census = Census(year=1940)

    datapath = 'data/XML'
    
    if file:
        print(f"Processing {os.path.join(datapath, file)}")
        census.parseXML(file)        
    else:
        files = list_files(datapath, "xml")        
        
        total=len(files)        
        c=1        
        for file in files:                                
            print(f"{c}/{total}: Processing {os.path.join(datapath, file)}")
            census.parseXML(os.path.join(datapath, file))
            c+=1
    
if __name__ == "__main__":     
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        main()
    

