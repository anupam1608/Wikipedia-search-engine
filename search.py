import re
import math
import sys
import timeit
import Stemmer
from collections import defaultdict

#offset = []
#titleOffset = []

stemmer = Stemmer.Stemmer('english')

##### Store stop words in a dictionary
with open('./stopwords.txt', 'r') as file :
    stop_words=file.read().split('\n')
    stop_words = set(stop_words)
    stop_dict = defaultdict(int)
for word in stop_words:
    stop_dict[word] = 1

def remove_special(text):
    text = re.sub(r'[^A-Za-z0-9]+', r' ', text) #Remove Special Characters
    return text
    
def removeStopwords(text):     # Stop Words Removal
    return [word for word in text if stop_dict[word] != 1 ]

def stem(text):
    return stemmer.stemWords(text)

def tokenize(text):
    text = text.encode("ascii", errors="ignore").decode()
    text = remove_special(text)
    return text.split()

def begin_search():
    print('-------- Search Engine Loading -------\n')
    f = open('./file1/fileNumbers.txt', 'r')
    n=f.read().strip()
    n=int(n)
    nfiles = n
    f.close()
    tem_val = len(titleOffset)
    titleFile = open('./file1/title.txt', 'r')
    fvocab = open('./file1/vocab.txt', 'r')
    inputQueryFile=sys.argv[1]
    #inputQueryFile="queries.txt"
    outputQueryFile="queries_op.txt"
    inFile=open(inputQueryFile,'r')
    outFile=open(outputQueryFile,"w")
    print('\nRelevant Results:\n')
    for query in inFile:
        #numQuery+=1
        start = timeit.default_timer()
        query = query.strip()
        query=query.split(',')
        K=int(query[0])
        query=query[1]
        query=query.strip()
        #print(query)
        query = query.lower()
        
        if re.match(r'[t|b|i|c|l]:', query):
            tempFields = re.findall(r'([t|b|c|i|l]):', query)
            words = re.findall(r'[t|b|c|i|l]:([^:]*)(?!\S)', query)
            fields=[]
            tokens=[]
            i=0
            while i<len(words):
                for word in words[i].split():
                    fields.append(tempFields[i])
                    tokens.append(word)
                i=i+1
            tokens = removeStopwords(tokens)
            tokens = stem(tokens)
            results,docFreq=query_fields(tokens,fields,fvocab)
            results = ranking(nfiles, 'f',results, docFreq)
        else:
            tokens = tokenize(query)
            tokens = removeStopwords(tokens)
            tokens = stem(tokens)
            results, docFreq = query_simple(fvocab,tokens)
            results = ranking(nfiles, 's',results, docFreq)
        if len(results) > 0:
            results = sorted(results, key=results.get, reverse=True)
            results = results[:K]
            for key in results:
                d_ID,title = findFileNo(titleOffset, key,0,tem_val, titleFile, 'int')
                title=' '.join(title)
                print(d_ID,title,sep=',')
                string=str(d_ID) +"," + title+'\n'
                outFile.write(string)
            #print('\n')
            end = timeit.default_timer()
            totalTime=end-start
            avgT=totalTime/K
            print(totalTime,avgT,sep=',')
            print("\n")
            string =str(totalTime)+' , '+str(avgT)+'\n\n'
            outFile.write(string)
    inFile.close()
    outFile.close()
        
def query_fields(words, fields, fvocab):
    docList = defaultdict(dict)
    docFreq = {}
    i=0
    while i < len(words):
        field = fields[i]
        word = words[i]
        mid,docs = findFileNo(offset, word,0, len(offset), fvocab)
        x=len(docs)
        if x > 0:
            j=0
            fileNo = docs[j]
            filename = './file1/' + field + str(fileNo) + '.txt'
            fieldFile = open(filename, 'r')
            df,returnedList = docFind(filename, word,fileNo, field, fieldFile)
            docFreq[word] = df
            docList[word][field] = returnedList
        i=i+1
    return docList, docFreq

def query_simple(fvocab,words):
    docFreq = {}
    fields=['t', 'b', 'i', 'c', 'l']
    docList = defaultdict(dict)
    for word in words:
        mid,docs = findFileNo(offset, word,0, len(offset), fvocab)
        x=len(docs)
        if  x > 0:
            i=0
            fileNo = docs[i]
            docFreq[word] = docs[i+1]
            for field in fields:
                filename = './file1/' + field + str(fileNo) + '.txt'
                fieldFile = open(filename, 'r')
                _,returnedList = docFind(filename, word,fileNo, field, fieldFile)
                docList[word][field] = returnedList
    return docList, docFreq

def findFileNo(offset, word,low, high,f, typ='str'):
    while low<high :
        mid = int((low + high) / 2)
        f.seek(offset[mid])
        w=f.readline()
        w=w.strip()
        w=w.split()
        wordPtr = w
        if typ == 'str':
            wx=wordPtr[0]
            if wx == word :
                return mid,wordPtr[1:]
            elif word <= wx:
                high = mid
            else:
                low = mid + 1
        else:
             wx=wordPtr[0]
             if int(wx) == int(word):
                return mid,wordPtr[1:]
             elif int(word) <=int(wx):
                high = mid
             else:
                low = mid + 1 
    return -1,[]

def docFind(filename, word,fileNo, field ,fieldFile):
    fieldOffset,docFreq = [],[]
    fileName = './file1/offset_' + field + fileNo + '.txt'
    with open(fileName) as file:
        for line in file:
            x=line.strip().split()
            offset=int(x[0])
            df=int(x[1])
            docFreq.append(df)
            fieldOffset.append(offset)
    mid,docList = findFileNo(fieldOffset, word,0, len(fieldOffset), fieldFile)
    return docFreq[mid],docList

def ranking(N,queryType,results,docFreq):
    tfidf=0.0
    if(queryType=='s'):
        values=[0.05,0.40,0.05,0.40,0.10]  #l,b,i,t,c
    else:
        values=[0.05,0.40,0.05,0.40,0.10]   
    docs=defaultdict(float)
    for key in docFreq:
        idf=0.0
        idf=float(N)/float(docFreq[key])
        docFreq[key]=math.log(idf)
        idf=idf
    for word in results:
        fieldWisePostingList=results[word]
        for field in fieldWisePostingList:
            if(len(field)>0):
                postingList=fieldWisePostingList[field]
                if(field=='l'):
                    factor=values[0]
                if(field=='b'):
                    factor=values[1]
                if(field=='i'):
                    factor=values[2]
                if(field=='t'):
                    factor=values[3]
                if(field=='c'):
                    factor=values[4]
                for i in range(0,len(postingList),2):
                    tf=1+math.log(float(postingList[i+1]))
                    idf=docFreq[word]
                    tfidf=tf*idf
                    docs[postingList[i]]=docs[postingList[i]]+float(tfidf*factor)
        return docs

def write_log_file(message):
    try:
        file1 = open("logfile.txt","a")
        file1.write(message)
        file1.write("\n")
        file1.close()
    except:
        print("Exception in write_log_file")

if __name__ == '__main__':
    offset=[]
    titleOffset=[]
    file_name = './file1/titleOffset.txt'
    with open(file_name, 'r') as f:
        for line in f:
            x=int(line.strip())
            titleOffset.append(x)
    file_name = './file1/offset.txt'
    with open(file_name, 'r') as f:
        for line in f:
            x=int(line.strip())
            offset.append(x)
    begin_search()
