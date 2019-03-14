import requests
import json
from itertools import izip, chain, repeat
import sys
import enlighten

def read_file_to_text(filePath):
    f = open(filePath, "r")
    return f.read()

def make_new_json_obj(idInt, numFields, startNum):
    #construct json object for update to solr

    thisMap = {}
    thisMap['id'] = idInt

    for field in range(numFields):
        thisMap['x-manyFieldsTest-' + hex(startNum)] = 'x-manyFieldsTest-' + hex(startNum)
        startNum += 1

    idInt += 1

    return thisMap

def get_latest_docnum(endpoint):
    #determine the last created doc name

    print 'Getting last Doc Number'
    for i in range(0, 100):
        print 'Making request. Trying ' + str(i + 1) + ' times.'
        try:
            r = requests.get(endpoint)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print err
            continue
        break

    content      = r.content
    contentMap   = json.loads(content)
    responses    = contentMap['response']
    highestNum   = responses['numFound']

    return highestNum

def get_latest_fieldnum(endpoint):
    #determine the highest hex number and return decimal equivalent

    print 'Getting last Hex Number'

    for i in range(0, 100):
        print 'Making request. Trying ' + str(i + 1) + ' times.'
        try:
            r = requests.get(endpoint)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print err
            continue
        break

    content      = r.content
    contentMap   = json.loads(content)
    responses    = contentMap['response']
    docsList      = responses['docs']
    highestNum   = 0

    #find highest hex and convert to decimal
    doc = docsList[0]
    for field in doc.keys():
        if field.startswith('x-manyFieldsTest-'):
            thisNumber = field.split('x-manyFieldsTest-')[1]
            decNum     = int(thisNumber, 16)
            if decNum > highestNum:
                highestNum = decNum

    return highestNum

def first_lower(s):
   #make first letter of string lowercase

   if len(s) == 0:
      return s
   else:
      return s[0].lower() + s[1:]

def get_config_map(filePath):
    #read configuration file to map

    return json.loads(read_file_to_text(filePath))

def update_collection(endpoint, docsJson):
    #create and send http request to desired endpoint

    headersObj = {'content-type': 'application/json'}

    print 'sending data to endpoint with ' + str(len(json.loads(docsJson))) + ' documents'

    for i in range(0, 100):
        print 'Making request. Trying ' + str(i + 1) + ' times.'
        try:
            r = requests.post(endpoint, data=docsJson, headers=headersObj)
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print err
            continue
        break

    print "Sent data to endpoint: " + endpoint
    print "Response status code: " + str(r.status_code)
    print ""
    print "============================================================="

def grouper(docsPerSubmission, docObjects, padvalue=None):
    #group sets of objects into arrays of chosen length

    return izip(*[chain(docObjects, repeat(padvalue, docsPerSubmission - 1))]* docsPerSubmission)

def remove_null_values(thisList):
    #remove all null values from list

    newList = list()
    for i in thisList:
        if i is not None:
            newList.append(i)

    return newList

def main():

    # get CLI args
    cmd_args    = sys.argv
    flagCommit  = False
    incremental = False

    # Go through CLI options, where argument value = cmd_args[opt + 1]
    for opt in range(len(cmd_args)):
        #this flag will set commit to true, regardless of config
        if cmd_args[opt] == '-c':
            flagCommit = True
        if cmd_args[opt] == '-i':
            #do incremental creation of field names
            #starting from last field created
            incremental  = True

    submitList   = []
    configMap    = get_config_map('./config.json')
    hostname     = configMap['hostname']
    protocol     = configMap['protocol']
    port         = configMap['port']
    collection   = configMap['collection']
    docsPerSub   = configMap['docsPerSubmission']
    fieldsPerDoc = configMap['fieldsPerDoc']
    totalFields  = configMap['totalFields']
    merge        = configMap['merge']

    #flag -c commit overrides config
    if flagCommit:
        commit = True
    else:
        commit       = configMap['commit']

    counter      = 1
    fieldsLeft   = totalFields
    lastFieldNum = 0

    #if incremental mode is enabled, start counter at highest number + 1
    #and last field num as highest number
    if incremental:
        print ''
        print 'Incremental Mode: '
        print ''
        counter      = get_latest_docnum(protocol + '://' + hostname + ':' + str(port) + '/solr/' + collection + '/select?q=*:*') + 1
        lastFieldNum = get_latest_fieldnum(protocol + '://' + hostname + ':' + str(port) + '/solr/' + collection + '/select?q=id:' + str(counter - 1)) + 1
        print 'Starting at document id ' + str(counter)
        print 'Starting at field number ' + str(lastFieldNum)
        print ''
        print '============================================================='
        print ''

    while fieldsLeft != 0:

        if fieldsLeft >= fieldsPerDoc:

            newDocJson = make_new_json_obj(counter, fieldsPerDoc, lastFieldNum)
            #increment doc id counter
            counter    += 1
            #decrement fields left to create
            fieldsLeft -= fieldsPerDoc
            #increment lastFieldNum by num fields per doc
            lastFieldNum += fieldsPerDoc

            submitList.append(newDocJson)

        else:
            newDocJson = make_new_json_obj(counter, fieldsLeft, lastFieldNum)
            #increment doc id counter
            counter    += 1
            #decrement fields left to create
            fieldsLeft -= fieldsLeft

            submitList.append(newDocJson)

    #group lists of documents by submit size
    groupedList = list(grouper(docsPerSub, submitList))

    #create progress bar
    pbar = enlighten.Counter(total=len(groupedList), desc='Making Solr Docs', unit='ticks')

    #Loop through groups of  document objects, serialize, and send as post to endpoint
    for group in groupedList:
        pbar.update()
        #remove any null values from group
        thisGroup = remove_null_values(group)

        #serialize list of docs to json
        thisPayload  = json.dumps(thisGroup)
        thisEndpoint = protocol + '://' + hostname + ':' + str(port) + '/solr/' + collection + '/update?commit=' + first_lower(str(commit))
        #iterate progress bar
        update_collection(thisEndpoint, thisPayload)

    print ''
    print 'Done'

if __name__ == '__main__':
    main()
