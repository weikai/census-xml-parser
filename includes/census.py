import re
import yaml
import mysql.connector
import xml.etree.ElementTree as ET 

class Census:
    table_state="state"
    table_county='county'
    table_city='city'
    table_edsummary="ed_summary "
    table_mapimage='mapimage'

    states={}
    cities={}
    counties={}

    dbconnect = None
    dbcursor = None
    year = None

    sql_county = f"INSERT INTO {table_county} (name) VALUES (%s)"
    sql_city = f"INSERT INTO {table_city} (name) VALUES (%s)"
    sql_ed_summary = f"INSERT INTO {table_edsummary} (edid, stateid, countyid, description,year) VALUES (%s, %s, %s, %s, %s)"
    sql_mapimage = f"INSERT INTO {table_mapimage} (stateid, countyid, cityid, edid, publication, rollnum, imgseq, filename, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    def __init__(self,dbconfig="database.yaml", year=1940):
        with open(dbconfig) as file:    
            db = yaml.load(file, Loader=yaml.FullLoader)
            self.dbconnect = mysql.connector.connect(
                host = db.get('dbhost'),
                user = db.get('dbuser'),
                password = db.get('dbpassword'),
                database = db.get('database')
            )
        self.dbcursor = self.dbconnect.cursor(buffered=True)
        self.year = year
        self.setup_states()
        self.setup_county()

        # truncate edsummary table
        self.dbcursor.execute(f"truncate {self.table_edsummary}")

        # truncate county table
        self.dbcursor.execute(f"truncate {self.table_county}")

        #truncate mapimage
        self.dbcursor.execute(f"truncate {self.table_mapimage}")
        
    #def __del__(self): 
        #self.dbconnect.commit()
        

    def parseXML(self,xmlfile):   
        # create element tree object 
        tree = ET.parse(xmlfile) 
    
        # get root element 
        root = tree.getroot()         
        path = self.process_node(root)
        self.dbconnect.commit()
        

    def process_node(self,node,data={}):
        # loop through all nodes recursively and process each elements

        data = data.copy()  
        # get state
        if node.tag == 'QC-By-Jurisdiction':
            state = node.get('state')
            data.update({'state':state})
            data.update({'stateid':self.states[state.lower()]})

        elif node.tag == 'county-summary':   # has ed-summary
            county = node.get('name')
            data.update({'county':county})
            if county.lower not in self.counties:
                val = (county,)                               
                self.dbcursor.execute(self.sql_county, val)
                self.counties.update({county.lower():self.dbcursor.lastrowid})
                data.update({'countyid':self.dbcursor.lastrowid})

        elif node.tag == 'city-summary':
            city = node.get('name')
            data.update({'city':city})
            data.update({'ed-list':node.get('ed-list')})
            if city.lower not in self.cities:
                val = (city,)                               
                self.dbcursor.execute(self.sql_city, val)
                self.cities.update({city.lower():self.dbcursor.lastrowid})
                data.update({'cityid':self.dbcursor.lastrowid})


        #data.update({"tag":node.tag, 'attributes':node.attrib, "node":node.text})    
        if node.tag == 'ed-summary':  # summary for county-summary     
            data.update({'ed':node.get("ed")})        
            data.update({'desc':node.find("T1224-description").text})            
            val = (node.get("ed"), data['stateid'],data['countyid'], node.find("T1224-description").text, self.year)
            self.dbcursor.execute(self.sql_ed_summary, val)   
        
        # check and process ed-summary first
        edsummary = node.findall('ed-summary')
        if edsummary:
            for el in edsummary:
                self.process_node(el, data)
        
        for el in node:                    
            if el.findall("[image]"):                     
                publication = re.sub("-.*",'',el.tag)                
                for image in el.iter("image"):  #find and process files                    
                    filename = re.sub("\..*$","", image.get('filename'))
                    namedata = filename.split("-")
                    if len(namedata) == 4:
                        rollnum = namedata[2]
                        imgseq = namedata[3]
                        
                    else:
                        rollnum = imgseq = 0
                        print(f"{image.get('filename')} not using standard file format")
                        
                    filename += ".jpg"                    
                    #print(data.get('ed'))
                    if data.get('ed'):
                        self.dbcursor.execute(f"SELECT id FROM {self.table_edsummary} WHERE edid = %s", (data.get('ed'),))
                        result = self.dbcursor.fetchone()
                        if result:                            
                            edid = result[0]                        
                    else:
                        edid = 0                    
                    val = (data.get('stateid'),data.get('countyid'), self.dictget('cityid',data,0), edid,publication,rollnum,imgseq,filename, self.year)                                        
                    #print(val)
                    self.dbcursor.execute(self.sql_mapimage, val)               
            elif el.tag != 'ed-summary':  #process other node except ed-summary
                self.process_node(el, data)

    def dictget(self, key, data, empty=''):
        if data and key in data:
            return data.get(key)
        return empty
    def setup_county(self):
        self.dbcursor.execute(f"SELECT * FROM {self.table_county}")
        results = self.dbcursor.fetchall()
        for county in results:                        
            self.counties.update({county[1].lower():county[0]})
        
    def setup_states(self):

        self.dbcursor.execute(f"SELECT * FROM {self.table_state}")        
        if self.dbcursor.rowcount > 50:
            results = self.dbcursor.fetchall()
            for state in results:
                self.states.update({state[2].lower():state[0]})                
        else:
            states=[]
            states.append(["Alabama","AL"])
            states.append(["Alaska","AK"])
            states.append(["American Samoa","AS"])
            states.append(["Arizona","AZ"])
            states.append(["Arkansas","AR"])
            states.append(["California","CA"])
            states.append(["Colorado","CO"])
            states.append(["Connecticut","CT"])
            states.append(["Delaware","DE"])
            states.append(["District of Columbia","DC"])
            states.append(["Florida","FL"])
            states.append(["Georgia","GA"])
            states.append(["Guam","GU"])
            states.append(["Hawaii","HI"])
            states.append(["Idaho","ID"])
            states.append(["Illinois","IL"])
            states.append(["Indiana","IN"])
            states.append(["Iowa","IA"])
            states.append(["Kansas","KS"])
            states.append(["Kentucky","KY"])
            states.append(["Louisiana","LA"])
            states.append(["Maine","ME"])
            states.append(["Maryland","MD"])
            states.append(["Massachusetts","MA"])
            states.append(["Michigan","MI"])
            states.append(["Minnesota","MN"])
            states.append(["Mississippi","MS"])
            states.append(["Missouri","MO"])
            states.append(["Montana","MT"])
            states.append(["Nebraska","NE"])
            states.append(["Nevada","NV"])
            states.append(["New Hampshire","NH"])
            states.append(["New Jersey","NJ"])
            states.append(["New Mexico","NM"])
            states.append(["New York","NY"])
            states.append(["North Carolina","NC"])
            states.append(["North Dakota","ND"])
            states.append(["Ohio","OH"])
            states.append(["Oklahoma","OK"])
            states.append(["Oregon","OR"])
            states.append(["Pennsylvania","PA"])
            states.append(["Trust Territory of the Pacific Islands","PC"])
            states.append(["Puerto Rico","PR"])
            states.append(["Rhode Island","RI"])
            states.append(["South Carolina","SC"])
            states.append(["South Dakota","SD"])
            states.append(["Tennessee","TN"])
            states.append(["Texas","TX"])
            states.append(["Utah","UT"])
            states.append(["Vermont","VT"])
            states.append(["Virginia","VA"])
            states.append(["Virgin Islands","VI"])
            states.append(["Washington","WA"])
            states.append(["West Virginia","WV"])
            states.append(["Wisconsin","WI"])
            states.append(["Wyoming","WY"])

            self.dbcursor.execute(f"truncate {self.table_state}")
            sql = f"INSERT INTO {self.table_state} (name, abbr) VALUES (%s, %s)"        
            for state in states:    
                val = (state[0], state[1])
                self.dbcursor.execute(sql, val)
                self.states.update({state[1].lower():self.dbcursor.lastrowid})
            
        