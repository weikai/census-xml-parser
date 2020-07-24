import yaml, re
import mysql.connector
import xml.etree.ElementTree as ET 

class Census:
    
    table_state="state"
    table_county='county'
    table_city='city'
    table_edsummary='ed_summary'
    table_mapimage='mapimage'
    table_recordtype='recordtype'

    settings_recordtypes = None    

    states={}
    cities={}
    counties={}
    recordtypes={}

    dbconnect = None
    dbcursor = None
    year = None

    sql_county = f"INSERT INTO {table_county} (name) VALUES (%s)"
    sql_city = f"INSERT INTO {table_city} (name) VALUES (%s)"
    sql_ed_summary = f"INSERT INTO {table_edsummary} (edid, stateid, countyid, description,year) VALUES (%s, %s, %s, %s, %s)"
    sql_mapimage = f"INSERT INTO {table_mapimage} (stateid, countyid, cityid, edid, publication, rollnum, imgseq, filename, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    def __init__(self,dbconfig="settings.yaml", year=1940):
        states=None
        with open(dbconfig) as file:    
            config = yaml.load(file, Loader=yaml.FullLoader)
            db = config.get('database')
            self.dbconnect = mysql.connector.connect(
                host = db.get('host'),
                user = db.get('user'),
                password = db.get('password'),
                database = db.get('database')
            )
        states=config.get('states')

        self.settings_recordtypes = config.get('recordtypes')
        
        self.dbcursor = self.dbconnect.cursor(buffered=True)
        self.year = year
        self.setup_recordtype()
        self.setup_states(states)
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

        # set data type to maps
        elif node.tag in self.settings_recordtypes.get('maps'):
            data.update({'type':'maps'})
            data.update({'typeid':self.recordtypes.get('maps')})
        
        # set record type to descriptons
        elif node.tag in self.settings_recordtypes.get('descriptions'):
            data.update({'type':'descriptions'})
            data.update({'typeid':self.recordtypes.get('descriptions')})

            if node.tag == 'T1224-description': # process and save description to db
                val = (data.get("ed"), data['stateid'],data['countyid'], node.text, self.year)
                self.dbcursor.execute(self.sql_ed_summary, val)   
        
        # set record type to schedules
        elif node.tag in self.settings_recordtypes.get('schedules'):
            #print('schedules')
            data.update({'type':'schedules'})
            data.update({'typeid':self.recordtypes.get('schedules')})
        
        #data.update({"tag":node.tag, 'attributes':node.attrib, "node":node.text})    
        elif node.tag == 'ed-summary':  # summary for county-summary     
            data.update({'ed':node.get("ed")})        
        
        
            
        
        
        # check and process ed-summary first
        edsummary = node.findall('ed-summary')
        if edsummary:
            for el in edsummary:
                self.process_node(el, data)
        #print(node.tag)
        #print(self.settings_recordtypes.get('descriptions'))
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
                    else:
                        edid = 0                    
                    val = (data.get('stateid'),data.get('countyid'), self.dictget('cityid',data,0), edid,publication,rollnum,imgseq,filename, self.year)
                    
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
        
    def setup_states(self, states):

        self.dbcursor.execute(f"SELECT * FROM {self.table_state}")        
        if self.dbcursor.rowcount > 50:
            results = self.dbcursor.fetchall()
            for state in results:
                self.states.update({state[2].lower():state[0]})                
        else:    
            self.dbcursor.execute(f"truncate {self.table_state}")
            sql = f"INSERT INTO {self.table_state} (name, abbr) VALUES (%s, %s)"        
            for state in states:    
                val = (state.split(','))                
                self.dbcursor.execute(sql, val)
                self.states.update({val[1].lower():self.dbcursor.lastrowid})
    
    def setup_recordtype(self):

        self.dbcursor.execute(f"SELECT * FROM {self.table_recordtype}")        
        if self.dbcursor.rowcount > 2:
            results = self.dbcursor.fetchall()
            for rtype in results:
                self.recordtypes.update({rtype[1].lower():rtype[0]})                
        else:            
            self.dbcursor.execute(f"truncate {self.table_recordtype}")
            sql = f"INSERT INTO {self.table_recordtype} (name, label) VALUES (%s, %s)"
            for rtype in self.settings_recordtypes.keys():    
                val = (rtype.lower(), rtype.capitalize())
                self.dbcursor.execute(sql, val)
                self.recordtypes.update({val[0]:self.dbcursor.lastrowid})        
        