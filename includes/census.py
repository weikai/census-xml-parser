import yaml, re
import mysql.connector
import xml.etree.ElementTree as ET 

class Census:
    
    table_state="state"
    table_county='county'
    table_city='city'
    table_city_ed='enumeration'
    table_edsummary='ed_summary'
    table_mapimage='map_image'
    table_recordtype='record_type'
    table_locale='city_state'

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
    sql_city_ed = f"INSERT INTO {table_city_ed} (stateid, countyid, cityid, edid) VALUES (%s, %s, %s, %s)"
    sql_ed_summary = f"INSERT INTO {table_edsummary} (ed, stateid, countyid, description,year, sortkey) VALUES (%s, %s, %s, %s, %s, %s)"
    sql_locale = f"INSERT INTO {table_locale} (stateid, countyid, cityid) VALUES (%s, %s, %s)"
    sql_mapimage = f"INSERT INTO {table_mapimage} (typeid, stateid, countyid, cityid, edid, publication, rollnum, imgseq, filename, year) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

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

        self.dbcursor.execute(f"truncate {self.table_city_ed}")

        self.dbcursor.execute(f"truncate {self.table_city}")
        self.dbcursor.execute(f"truncate {self.table_locale}")
        
    #def __del__(self): 
        #self.dbconnect.commit()
        

    # parse xml document
    def parseXML(self,xmlfile):   
        # create element tree object 
        tree = ET.parse(xmlfile) 
    
        # get root element 
        root = tree.getroot()         
        path = self.process_node(root)
        self.dbconnect.commit()
        
    # recursive function to process each node elements    
    def process_node(self,node,data={}):
        # loop through all nodes recursively and process each elements
        parent = data.get('parent')
        data = data.copy()  
        data.update({'parent': node})

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
            #data.update({'ed-list':node.get('ed-list')})
            if city.lower not in self.cities:
                val = (city,)                               
                self.dbcursor.execute(self.sql_city, val) #add new city to db
                self.cities.update({city.lower():self.dbcursor.lastrowid})
                data.update({'cityid':self.dbcursor.lastrowid})

            # add state, count and city relations
            val=( data.get('stateid'), data.get('countyid'), data.get('cityid'))
            self.dbcursor.execute(self.sql_locale, val)

            #add ed-list to city
            for ed in re.split(r'[;,]',node.get('ed-list')):
                
                if ed:
                    eid = self.find_ed_id(ed, data.get('stateid'), data.get('countyid'))
                    if eid:
                        self.insert_enumeration(data.get('stateid'), data.get('countyid'), data.get('cityid'), eid)
                    else:
                        print("Can't find ed record in ed_summary")
                    


        # set data type to maps
        elif node.tag in self.settings_recordtypes.get('maps'):
            data.update({'type':'maps'})
            data.update({'typeid':self.recordtypes.get('maps')})
        
        # set record type to descriptons
        elif node.tag in self.settings_recordtypes.get('descriptions'):
            data.update({'type':'descriptions'})
            data.update({'typeid':self.recordtypes.get('descriptions')})

            if node.tag == 'T1224-description': # process and save description to db
                sortid=''
                for g  in data.get("ed").split('-'):
                    sortid += str(g).zfill(3)
                
                val = (data.get("ed"), data['stateid'],data['countyid'], node.text, self.year,sortid)
                self.dbcursor.execute(self.sql_ed_summary, val)   
        
        # set record type to schedules
        elif node.tag in self.settings_recordtypes.get('schedules'):
            #print('schedules')
            data.update({'type':'schedules'})
            data.update({'typeid':self.recordtypes.get('schedules')})
        
        #data.update({"tag":node.tag, 'attributes':node.attrib, "node":node.text})    
        elif node.tag == 'ed-summary':  # summary for county-summary     
            data.update({'ed':node.get("ed")})        
        
        elif node.tag == 'image': # process image node                   
            publication = re.sub("-.*",'',parent.tag)
            filename = re.sub("\..*$","", node.get('filename'))
            namedata = filename.split("-")
            if len(namedata) == 4:
                rollnum = namedata[2]
                imgseq = namedata[3]
                
            else:
                rollnum = imgseq = 0
                print(f"{node.get('filename')} not using standard file format")
                
            filename += ".jpg"                                
            if data.get('ed'):                
                eid = self.find_ed_id( data.get('ed'), data.get('stateid'), data.get('countyid') )                

                #print( data.get('ed'), data.get('stateid'), data.get('countyid') ,  self.dictget('cityid',data,0), eid)
                if eid:                    
                    edid = self.insert_enumeration(data.get('stateid'), data.get('countyid'), data.get('cityid'),eid)
                else:
                    print(f"ed {data.get('ed')} not found")
            else:                
                edid=None
                        
            #print(f"ed: {data.get('ed')}; city: {data.get('cityid')}")
            val = (data.get('typeid'),data.get('stateid'),data.get('countyid'), self.dictget('cityid',data,None), edid,publication,rollnum,imgseq,filename, self.year)

            #print(val)
            
            self.dbcursor.execute(self.sql_mapimage, val)    
        
        
        # check and process ed-summary first
        edsummary = node.findall('ed-summary')
        if edsummary:
            for el in edsummary:
                self.process_node(el, data)        

        for el in node:
            if el.tag != 'ed-summary':  #process other node except ed-summary
                self.process_node(el, data)  # call self recursively to process children nodes

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
                #print(val);
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

    def insert_enumeration(self, stateid, countyid, cityid, edid): 
        
        if cityid: # skip insert if edid with same city county and state is already exists
            self.dbcursor.execute(f"SELECT id, cityid FROM {self.table_city_ed} WHERE edid = %s and stateid = %s and countyid = %s and cityid = %s", ( edid, stateid, countyid, cityid))        
            result = self.dbcursor.fetchone()        
            if result:
                return result[0]

        self.dbcursor.execute(f"SELECT id, cityid FROM {self.table_city_ed} WHERE edid = %s and stateid = %s and countyid = %s", ( edid, stateid, countyid))
        result = self.dbcursor.fetchone()        
        if result:   
            id = result[0]    
            
            if cityid and not result[1]:  # update the city record if city is empty
                self.dbcursor.execute(f"UPDATE {self.table_city_ed} SET cityid = %s WHERE id = %s", ( result[0], cityid))               
            return id
            
        #add new record
        val=(stateid, countyid, cityid, edid )         
        self.dbcursor.execute(self.sql_city_ed, val) 
        return self.dbcursor.lastrowid
    
    def find_ed_id(self, ed, stateid, countyid):
        self.dbcursor.execute(f"SELECT id FROM {self.table_edsummary} WHERE ed = %s and  stateid = %s and countyid = %s", (ed,stateid, countyid))
        result = self.dbcursor.fetchone()
        if result:                                                
            return result[0]
        else:
            return None