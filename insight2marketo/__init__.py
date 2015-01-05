import mktorest
import os
from datetime import datetime, timedelta
import time
import pymysql as mdb
import requests
from hashlib import sha1
import hmac

def getMktoClient():
    munchkinid = os.environ.get('MKTO_MUNCHKIN_ID', '383-VLX-233')
    clientid =  os.environ.get('MKTO_CLIENT_ID', '9b49688c-c0cf-4903-a691-c7fc28ecbd42')
    clientsecret = os.environ.get('MKTO_CLIENT_SECRET', 'QFTuXjairHTz3capR6V8iZ2OPsdwpc9u')
    return mktorest.Client(munchkinid, clientid, clientsecret)

class DBHelper():
    def __init__(self):
        dbhost = os.environ.get('DB_HOST', 'localhost')
        dbuser = os.environ.get('DB_USERNAME', 'testuser')
        dbpw = os.environ.get('DB_PASSWORD', 'test123')
        dbdb = os.environ.get('DB_NAME', 'ssmirror')
        self.leadTable = os.environ.get('DB_LEAD_TABLE', 'arc_marketo_upload') #arc_marketo_upload
        try:
            self.con =  mdb.connect(dbhost, dbuser, dbpw, dbdb, autocommit=True)
            self.cur = self.con.cursor(mdb.cursors.DictCursor)
            self.cur.execute('SELECT VERSION()')
            print 'Database retrieved, MySQL Version: %r' % self.cur.fetchall()[0]['VERSION()']
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            return False

    def close(self):
        self.con.close()

    def queryLeadsByFlag(self, ptmFlagValue, limit=None):
        #Takes in a list of PushToMarketo Flag values to query the leads table by, returns a list of python dictionaries describing the leads returned
        ptmquery = ''
        for val in ptmFlagValue:
            ptmquery = ptmquery + "pushToMarketo = "+str(val)+" OR "
        ptmquery=ptmquery.rstrip(' OR')
        if limit:
            ptmquery=ptmquery+" LIMIT "+str(limit)
        query = 'SELECT * FROM '+self.leadTable+' WHERE '+ptmquery
        #print query
        self.cur.execute(query)
        return self.cur.fetchall()

    def updateStatus(self, userIds, uploadStatus, ptm=None):
        uploadtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        stringListIds = ''
        for userId in userIds:
            stringListIds+=str(userId)+','
        stringListIds=stringListIds.rstrip(',')
        if ptm!=None:
            ptmQ="', pushToMarketo='"+str(ptm)
        else:
            ptmQ=''
        query = "UPDATE "+self.leadTable+" SET uploadStatus='"+uploadStatus+"', uploadDateTime='"+uploadtime+ptmQ+"' WHERE userID IN ("+stringListIds+")"
        #print query
        self.cur.execute(query)
        return self.cur.fetchall()

    def resetBulk(self, uploadStatus, ptm=None):
            uploadtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if ptm!=None:
                ptmQ="', pushToMarketo='"+str(ptm)
            else:
                ptmQ=''
            query = "UPDATE "+self.leadTable+" SET uploadStatus='"+uploadStatus+"', uploadDateTime='"+uploadtime+ptmQ+"' WHERE pushToMarketo=2"
            #print query
            self.cur.execute(query)
            return self.cur.fetchall()

def mktoAssociateLead(email, cookie, retry=False):
    if cookie.find('token')>-1:
        token = cookie.rsplit(':', 1)[1]
    else:
        token = cookie
    munchId = os.environ.get('MKTO_MUNCHKIN_ID', '383-VLX-233')
    munchKey = os.environ.get('MKTO_MUNCHKIN_KEY', 'fortesting')
    hashed = sha1(munchKey+email).hexdigest()
    timestamp=int(time.time()*1000)
    url = 'http://'+munchId+'.mktoresp.com/webevents/associateLead'
    params={
        '_mchAtEmail':email,
        '_mchNc':timestamp,
        '_mchKy':hashed,
        '_mchId':munchId,
        '_mchTk':token,
        '_mchHo':'smartsheet.com',
        '_mchRu':'/',
        '_mchPc':'http:',
        '_mchVr':'147'
    }
    try:
        response=requests.get(url, params=params)
        if response.status_code==200:
            return True
        else:
            print 'Error Associating '+Email+' to '+token
            print 'Error Code '+response.status_code+': '+response.text
    except ConnectionError:
        if retry:
            print '2nd Connection Error associating '+Email+' to '+token+', skipping'
            return False
        else:
            print 'Connection Error associating '+Email+' to '+token+', retrying...'
            return mktoAssociateLead(email, cookie, retry=True)

def parsemap(filename):
    f = open(filename, 'r')
    f.readline() #dump the header
    pymap = {}
    for line in f:
        sline = line.split(',')
        pymap[sline[1].strip()]=sline[0].strip()
    f.close()
    #RETURN DICTIONARY: {'DB NAME': ('MARKETO NAME', dateflag)} where dateflag is 1 or 0
    return pymap

def convertKeysFixDates(inMap, inDict):
    outDict={}
    for key in inDict:
        if isinstance(inDict[key], datetime):
            inDict[key]=inDict[key].isoformat()
        if inDict[key]==None:
            inDict[key]=''
        if key in inMap:
            outDict[inMap[key]]=str(inDict[key])
    return outDict

def sendLeadsRecord(leads, marketo, action=None, lookup=None, retryCount=0):
    try:
        response = marketo.createUpdateLeads(leads, lookup=lookup, action=action)
        failed=[]
        succeeded=[]
        for ii in range(len(response)):
            lead = response[ii]
            if lead['status']=='failed' or lead['status']=='skipped':
                failed.append(leads[ii]['userID__c_contact'])
            else:
                succeeded.append(leads[ii]['userID__c_contact'])
        return failed, succeeded
    except Exception:
        print Exception
        if retryCount<3:
            print 'Marketo Sync Error, Retrying...'
            retryCount+=1
            sendLeadsRecord(leads, marketo, action=action, lookup=lookup, retryCount=retryCount)
        else:
            print 'Retries failed, skipping...'
            for lead in leads:
                failed.append(lead['userID__c_contact'])
            return failed, succeeded

def syncPeriodic():
    if os.environ.get('SUSPEND_PERIODIC', '0') == '0':
        jobstart = datetime.now()
        print ''
        print jobstart.strftime('%a %d-%b-%Y %H:%M:%S')+' - Starting Periodic Data sync to Marketo...'
        print ''
        print 'Retrieving Database'
        db = DBHelper()
        
        print 'Retrieving Field Map'
        fieldmap = parsemap('fieldmap.csv')
        
        print 'Initializing Marketo Client'
        marketo = getMktoClient()
        
        created=[]
        updated=[]
        failed=[]

        #PTM = 3
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Updating Leads...'
        leads=db.queryLeadsByFlag([3])
        leadcache=0
        leadsToSend=[]
        for lead in leads:
            leadsToSend.append(convertKeysFixDates(fieldmap, lead))
            leadcache+=1
            if leadcache == 300:
                print 'Updating '+str(leadcache)+' leads...'
                #the following call will only update existing leads, deduplication on userId
                newfails, newsucceeds=sendLeadsRecord(leadsToSend, marketo, action='updateOnly', lookup='userID__c_contact')
                succeeded+=newsucceeds
                failed+=newfails
                leadsToSend=[]
                leadcache=0
        print 'Updating '+str(leadcache)+' leads...'
        newfails, newsucceeds=sendLeadsRecord(leadsToSend, marketo, action='updateOnly', lookup='userID__c_contact')
        updated+=newsucceeds
        failed+=newfails

        #PTM = 4
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Creating New Leads...'
        leads = db.queryLeadsByFlag([4])
        leadcache=0
        leadsToSend=[]
        for lead in leads:
            leadsToSend.append(convertKeysFixDates(fieldmap, lead))
            leadcache+=1
            if leadcache == 300:
                print 'Creating '+str(leadcache)+' leads...'
                #The following call uses default parameters, i.e. dedupe on email and create or update as appropriate
                newfails, newsucceeds=sendLeadsRecord(leadsToSend, marketo)
                succeeded+=newsucceeds
                failed+=newfails
                leadsToSend=[]
                leadcache=0
        print 'Creating '+str(leadcache)+' leads...'
        newfails, newsucceeds=sendLeadsRecord(leadsToSend, marketo)
        created+=newsucceeds
        failed+=newfails
        
        #PTM = 4 Tracking cookie association
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Associating tracking cookies...'
        for lead in leads:
            if lead['marketoTrackingCookie']:
                mktoAssociateLead(lead['emailAddress'], lead['marketoTrackingCookie'])

        #Update Database
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Recording Successes and Failures...'
        if failed:
            db.updateStatus(failed, 'FAILED')
        if created:
            db.updateStatus(created, 'CREATED', ptm=0)
        if updated:
            db.updateStatus(updated, 'UPDATED', ptm=0)

        #close out job
        db.close()
        jobend = datetime.now()
        jobtime = (jobend-jobstart).total_seconds()
        print ''
        print jobend.strftime('%a %d-%b-%Y %H:%M:%S')+' - Marketo Sync complete, total execution time: '+str(jobtime) + ' seconds'
        print ''
    else:
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Periodic Sync skipped, batch update in progress...'

def leadsToCsv(db, filename, ptmflag):
    leads=db.queryLeadsByFlag([ptmflag])
    print 'Preparing '+str(len(leads))+' leads...'
    fieldmap = parsemap('fieldmap.csv')
    mktofields=fieldmap.values()
    numfields=len(mktofields)
    csvheader=','.join(mktofields)
    csvheader
    f=open(filename, 'w')
    f.write(csvheader+os.linesep)
    for lead in leads:
        line=['']*numfields
        for key in lead:
            if key in fieldmap:
                if isinstance(lead[key], datetime):
                    lead[key]=lead[key].isoformat()
                if lead[key]==None:
                    lead[key]=''
                line[mktofields.index(fieldmap[key])]=str(lead[key])
        f.write(','.join(line)+os.linesep)
    f.close()

def syncBulk():
    os.environ['SUSPEND_PERIODIC'] = '1'
    jobstart = datetime.now()
    print ''
    print jobstart.strftime('%a %d-%b-%Y %H:%M:%S')+' - Starting Bulk Update of Marketo...'
    print ''
    print 'Retrieving Database'
    db = DBHelper()
    
    print 'Initializing Marketo Client'
    marketo = getMktoClient()
    
    updated=[]
    failed=[]
    csvname=os.environ.get('TMP_CSV_NAME', 'tmpdir/leads.csv')
    #PTM = 2
    print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Copying Leads to temporary file...'
    leadsToCsv(db, csvname, 2)
    
    print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Sending file to Marketo...'
    retryCount=0
    result=False
    while retryCount<1:
        if retryCount==3:
            print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+'Retries failed, cancelling bulk import...'
            retryCount+=1
        else:
            try:
                result = marketo.listimport(csvname)[0][0]
                retryCount=5
            except Exception, e:
                print str(e)
                print 'Marketo API Error, Retrying...'
                retryCount+=1

    if result:
        jobid=result['batchId']
        print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Import Id: '+str(jobid)+', Waiting for marketo import completion...'
        jobstatus=result['status']
        while jobstatus!="Complete" and jobstatus!="Failed":
            time.sleep(5)
            result=marketo.getImportLeadStatus(jobid)[0][0]
            jobstatus=result['status']
        if jobstatus=='Complete':
            if result['numOfRowsFailed']>0:
                failurefile=marketo.getImportFailureFile(jobid)
                print failurefile
            print datetime.now().strftime('%a %d-%b-%Y %H:%M:%S')+' - Import Completed, Recording Successes...'
            db.resetBulk('UPDATED', ptm=0)
        else:
            print 'Job Failed, Unknown error with Marketo api.'        
    
    #close out job
    db.close()
    jobend = datetime.now()
    jobtime = (jobend-jobstart).total_seconds()
    print ''
    print jobend.strftime('%a %d-%b-%Y %H:%M:%S')+' - Marketo Bulk Update complete, total execution time: '+str(jobtime) + ' seconds'
    print ''
    os.environ['SUSPEND_PERIODIC'] = '0'

if __name__=='__main__':
    pass
    #syncPeriodic()
    #syncBulk()