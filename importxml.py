from includes.census import *

def main():     
    census = Census(dbconfig="/home/weikai/database.yaml", year=1940)
    # parse xml file 
    census.parseXML('data/XML/AK.xml')  
    #census.parseXML('data/XML/AL.xml')
if __name__ == "__main__": 
  
    # calling main function 
    main() 
    

