import requests
import json

def readFileToText(filePath):
    f = open(filePath, "r")
    return f.read()

def makeNewJsonObj(idInt, numFields, startNum):

    thisMap = {}
    thisMap['id'] = idInt

    for field in range(numFields):

        thisMap['x-manyFieldsTest-' + hex(startNum)] = 'x-manyFieldsTest-' + hex(startNum)
        startNum += 1

    idInt += 1

    return thisMap

def getConfigMap(filePath):
    configMap = json.loads(readFileToText(filePath))
    return configMap

def updateCollection(endpoint, docsJson):
    headersObj = {'content-type': 'application/json'}
    r = requests.post(endpoint, data=docsJson, headers=headersObj)
    return r.status_code

def main():

    submitList   = []
    configMap    = getConfigMap('./config.json')
    hostname     = configMap['hostname']
    protocol     = configMap['protocol']
    docsPerSub   = configMap['docsPerSubmission']
    fieldsPerDoc = configMap['fieldsPerDoc']
    totalFields  = configMap['totalFields']
    merge        = configMap['merge']
    counter      = 1
    fieldsLeft   = totalFields
    lastFieldNum = 0

    while fieldsLeft != 0:

        if fieldsLeft >= fieldsPerDoc:

            newDocJson = makeNewJsonObj(counter, fieldsPerDoc, lastFieldNum)
            #increment doc id counter
            counter    += fieldsPerDoc
            #decrement fields left to create
            fieldsLeft -= fieldsPerDoc
            #increment lastFieldNum by num fields per doc
            lastFieldNum += fieldsPerDoc

            submitList.append(newDocJson)

        else:
            newDocJson = makeNewJsonObj(counter, fieldsLeft, lastFieldNum)
            #increment doc id counter
            counter    += fieldsPerDoc
            #decrement fields left to create
            fieldsLeft -= fieldsLeft

            submitList.append(newDocJson)

    print json.dumps(submitList)


if __name__ == '__main__':
    main()
