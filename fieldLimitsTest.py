import requests
import sys
import json
from itertools import izip, chain, repeat
import sys, getopt

def readFileToText(filePath):
    f = open(filePath, "r")
    return f.read()

def makeNewJsonObj(idInt, numFields, startNum):

    #construct json object for update to solr
    thisMap = {}
    thisMap['id'] = idInt

    for field in range(numFields):
        thisMap['x-manyFieldsTest-' + hex(startNum)] = 'x-manyFieldsTest-' + hex(startNum)
        startNum += 1

    idInt += 1

    return thisMap

def first_lower(s):
   #make first letter of string lowercase
   if len(s) == 0:
      return s
   else:
      return s[0].lower() + s[1:]

def getConfigMap(filePath):
    #read configuration file to map
    return json.loads(readFileToText(filePath))

def updateCollection(endpoint, docsJson):

    #create and send http request to desired endpoint
    headersObj = {'content-type': 'application/json'}

    try:
        r = requests.post(endpoint, data=docsJson, headers=headersObj)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print err
        sys.exit(1)

    print "Sent data to endpoint: " + endpoint
    print "Response status code: " + str(r.status_code)

def grouper(docsPerSubmission, docObjects, padvalue=None):

    #group sets of objects into arrays of chosen length
    return izip(*[chain(docObjects, repeat(padvalue, docsPerSubmission - 1))]* docsPerSubmission)

def removeNullValues(thisList):

    #remove all null values from list
    newList = list()
    for i in thisList:
        if i is not None:
            newList.append(i)

    return newList

def main():

    # get CLI args
    cmd_args   = sys.argv
    flagCommit = False

    # Go through CLI options, where argument value = cmd_args[opt + 1]
    for opt in range(len(cmd_args)):
        #this flag will set commit to true, regardless of config
        if cmd_args[opt] == '-c':
            flagCommit = True

    submitList   = []
    configMap    = getConfigMap('./config.json')
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

    while fieldsLeft != 0:

        if fieldsLeft >= fieldsPerDoc:

            newDocJson = makeNewJsonObj(counter, fieldsPerDoc, lastFieldNum)
            #increment doc id counter
            counter    += 1
            #decrement fields left to create
            fieldsLeft -= fieldsPerDoc
            #increment lastFieldNum by num fields per doc
            lastFieldNum += fieldsPerDoc

            submitList.append(newDocJson)

        else:
            newDocJson = makeNewJsonObj(counter, fieldsLeft, lastFieldNum)
            #increment doc id counter
            counter    += 1
            #decrement fields left to create
            fieldsLeft -= fieldsLeft

            submitList.append(newDocJson)

    #group lists of documents by submit size
    groupedList = list(grouper(docsPerSub, submitList))

    for group in groupedList:

        #remove any null values from group
        thisGroup = removeNullValues(group)

        #serialize list of docs to json
        thisPayload  = json.dumps(thisGroup)
        thisEndpoint = protocol + '://' + hostname + ':' + str(port) + '/solr/' + collection + '/update?commit=' + first_lower(str(commit))
        updateCollection(thisEndpoint, thisPayload)

        print ''
        print 'Done'

if __name__ == '__main__':
    main()
