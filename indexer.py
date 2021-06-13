import xml.sax
from collections import defaultdict
import timeit
from unidecode import unidecode
import sys
import re
import Stemmer
import heapq
import operator
import os
import shutil
import pdb
import threading
from tqdm import tqdm


stemmer = Stemmer.Stemmer('english')
#stemmer = PorterStemmer()
##### Store stop words in a dictionary
with open('./stopwords.txt', 'r') as file :
    stop_words=file.read().split('\n')
    stop_words = set(stop_words)
    stop_dict = defaultdict(int)
for word in stop_words:
    stop_dict[word] = 1
def removeSpecial(text):
	text = re.sub(r'[^A-Za-z0-9]+', r' ', text) #Remove Special Characters
	return text
def tokenize(text):                    # Tokenization
    global tokensCount_in_dump
    text = text.encode("ascii", errors="ignore").decode()
    text = removeSpecial(text)
    tokensCount_in_dump+=len(text.split())
    return text.split()
def removeStopwords(text):     # Stop Words Removal
    return [word for word in text if stop_dict[word] != 1 ]
def stem(text):  # Stemming
	return stemmer.stemWords(text)
    
def process_title(text):
    data = tokenize(text)
    data = removeStopwords(data)
    data = stem(data)
    return data
def process_body(text):
    data = re.sub(r'\{\{.*\}\}', r' ', text)
    data = tokenize(data)
    data = removeStopwords(data)
    data = stem(data)
    return data
def process_info(text):
    info = []
    data = text.split('\n')
    flag = -1
    st="}}"
    for line in data:
        if re.match(r'\{\{infobox', line):
            x=re.sub(r'\{\{infobox(.*)', r'\1', line)
            flag = 0
            info.append(x)
        elif flag == 0:
            if line == st:
                flag = -1
                continue
            info.append(line)
    data = tokenize(' '.join(info))
    data = removeStopwords(data)
    data = stem(data)
    return data
def process_categories(text):
    categories = []
    data = text.split('\n')
    for line in data:
        if re.match(r'\[\[category', line):
            x=re.sub(r'\[\[category:(.*)\]\]', r'\1', line)
            categories.append(x)
    data = tokenize(' '.join(categories))
    data = removeStopwords(data)
    data = stem(data)
    return data
def process_links(text):
    links = []
    data = text.split('\n')
    for line in data:
        if re.match(r'\*[\ ]*\[', line):
            links.append(line)
    data = tokenize(' '.join(links))
    data = removeStopwords(data)
    data = stem(data)
    return data

def processText(text, title):
    #### References,Links,Categories are below references & info,body,tilte above references.
    title= title.lower()
    title = process_title(title)
    
    text=text.lower()
    temp1=text.split("==references==")
    temp2=text.split("== references ==")
    if len(temp1)!=1:
        links=process_links(temp1[1])
        categories=process_categories(temp1[1])
        body=process_body(temp1[0])
        infobox=process_categories(temp1[0])
    elif len(temp2)!=1:
        links=process_links(temp2[1])
        categories=process_categories(temp2[1])
        body=process_body(temp2[0])
        infobox=process_categories(temp2[0])
    else:
        links=[]
        categories=[]
        body=process_body(temp1[0])
        infobox=process_categories(temp1[0])

    return title, body, infobox, categories, links

#Indexer
def Indexer(title, body, info, categories, links):
    global p_cnt
    global PostList
    global docID
    global f_cnt
    global offset
    ID = p_cnt
    
    dictALLWords=defaultdict(int)   
    dictCurrent=defaultdict(int)
    for word in title:
        dictCurrent[word]+=1
        dictALLWords[word]+=1
    title=dictCurrent
    
    dictCurrent=defaultdict(int)
    for word in body:
        dictCurrent[word]+=1
        dictALLWords[word]+=1
    body=dictCurrent
    
    dictCurrent=defaultdict(int)
    for word in links:
        dictCurrent[word]+=1
        dictALLWords[word]+=1
    links=dictCurrent
    
    dictCurrent=defaultdict(int)
    for word in info:
        dictCurrent[word]+=1
        dictALLWords[word]+=1
    info=dictCurrent
    
    dictCurrent=defaultdict(int)
    for word in categories:
        dictCurrent[word]+=1
        dictALLWords[word]+=1
    categories=dictCurrent
    
    for word in dictALLWords.keys():
        t=title[word]
        b=body[word]
        i=info[word]
        c=categories[word]
        l=links[word]
        string="d"+str(ID)
        if(t>=1):
            string+="t"+str(t)
        if(b>=1):
            string+="b"+str(b)
        if(i>=1):
            string+="i"+str(i)
        if(c>=1):
            string+="c"+str(c)
        if(l>=1):
            string+="l"+str(l)
        PostList[word].append(string)
    p_cnt += 1
    if p_cnt%50000== 0:
        offset = writeinfile(PostList, docID, f_cnt , offset)
        f_cnt = f_cnt + 1
        docID = {}
        PostList=defaultdict(list)
    	
def writeinfile(PostList, docID, f_cnt , offset):	
    data = []
    d_offset = []    
    p_offset = offset
    docIDList=sorted(docID)
    for key in docIDList:
        a=str(key)
        b=docID[key].strip()
        temp=a+' '+b
        if(len(temp)>0):
            p_offset = p_offset + len(temp) +1
        else:
            p_offset = p_offset+1
        data.append(temp)
        newPageOffset=p_offset
        d_offset.append(str(newPageOffset))
    fileName1 = './files/titleOffset.txt'
    with open(fileName1, 'a') as f:
        f.write('\n'.join(d_offset))
        f.write('\n')
    fileName2 = './files/title.txt'
    with open(fileName2, 'a') as f:
        f.write('\n'.join(data))
        f.write('\n')
    data = []
    keysPostList=sorted(PostList.keys())
    for key in keysPostList:
        string=key+' '+' '.join(PostList[key])
        data.append(string)
    fileName = './files/index' +str(f_cnt)+'.txt'
    with open(fileName, 'w') as f:
        f.write('\n'.join(data))
    return  newPageOffset

class writeThread(threading.Thread):
    def __init__(self, field, data, offset, count):
        threading.Thread.__init__(self)
        self.data = data
        self.field = field
        self.count = count
        self.offset = offset
        
    def run(self):
        fileName='./files/'+self.field+str(self.count)+'.txt'
        with open(fileName, 'w') as f:
            f.write('\n'.join(self.data))
        fileName='./files/offset_'+self.field+str(self.count)+'.txt'
        with open(fileName, 'w') as f:
            f.write('\n'.join(self.offset))

def final_write(data, finalCount, offsetSize):
    distinctWords = []
    offset = []
    title = defaultdict(dict)
    link = defaultdict(dict)
    info = defaultdict(dict)
    category = defaultdict(dict)
    body = defaultdict(dict)
    for key in tqdm(sorted(data.keys())):
        temp = []
        docs = data[key]
        i=0
        while(i<len(docs)):
            posting = docs[i]
            temp = re.sub(r'.*c([0-9]*).*', r'\1', docs[i])
            docID = re.sub(r'.*d([0-9]*).*', r'\1', docs[i])
            
            if len(temp)>0 and posting != temp:
                category[key][docID] = float(temp)
            temp = re.sub(r'.*i([0-9]*).*', r'\1', docs[i])
        
            if len(temp)>0 and posting != temp:
                info[key][docID] = float(temp)
            temp = re.sub(r'.*l([0-9]*).*', r'\1', docs[i])
        
            if len(temp)>0 and posting != temp:
                link[key][docID] = float(temp)
            temp = re.sub(r'.*b([0-9]*).*', r'\1', docs[i])

            if len(temp)>0 and posting != temp:
                body[key][docID] = float(temp)
            temp = re.sub(r'.*t([0-9]*).*', r'\1', docs[i])
            if len(temp)>0 and posting != temp:
                title[key][docID] = float(temp)
            i=i+1
        string = key + ' ' + str(finalCount) + ' ' + str(len(docs))
        offset.append(str(offsetSize))
        offsetSize += len(string) + 1
        distinctWords.append(string)
    
    titleData,linkData,bodyData,infoData,categoryData=[],[],[],[],[]
    titleOffset,linkOffset,bodyOffset,infoOffset, categoryOffset=[],[],[],[],[]
    prevTitle,prevLink,prevBody,prevInfo,prevCategory=0,0,0,0,0
    
    for key in tqdm(sorted(data.keys())):
        if key in link:
            docs = link[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(link[key][doc]) + ' '
            linkData.append(string)
            linkOffset.append(str(prevLink) + ' ' + str(len(docs)))
            currLength=len(string)
            prevLink += currLength + 1
            
        if key in category:
            docs = category[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(category[key][doc]) + ' '
            categoryData.append(string)
            categoryOffset.append(str(prevCategory) + ' ' + str(len(docs)))
            currLength=len(string)
            prevCategory += currLength + 1
        
        if key in title:
            docs = title[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(title[key][doc]) + ' '
            titleData.append(string)
            titleOffset.append(str(prevTitle) + ' ' + str(len(docs)))
            currLength=len(string)
            prevTitle += currLength + 1
        
        if key in info:
            docs = info[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(info[key][doc]) + ' '
            infoData.append(string)
            infoOffset.append(str(prevInfo) + ' ' + str(len(docs)))
            currLength=len(string)
            prevInfo += currLength + 1
        
        if key in body:
            docs = body[key]
            docs = sorted(docs, key = docs.get, reverse=True)
            string = key + ' '
            for doc in docs:
                string += doc + ' ' + str(body[key][doc]) + ' '
            bodyData.append(string)
            bodyOffset.append(str(prevBody) + ' ' + str(len(docs)))
            currLength=len(string)
            prevBody += currLength + 1
        
    thread = []
    thread.append(writeThread('t', titleData, titleOffset, finalCount))
    thread.append(writeThread('b', bodyData, bodyOffset, finalCount))
    thread.append(writeThread('i', infoData, infoOffset, finalCount))
    thread.append(writeThread('c', categoryData, categoryOffset, finalCount))
    thread.append(writeThread('l', linkData, linkOffset, finalCount))
    
    totalThread=5
    threadNo=0
    while(threadNo<totalThread):
        thread[threadNo].start()
        threadNo=threadNo+1
    threadNo=0
    while(threadNo<totalThread):
        thread[threadNo].join()
        threadNo=threadNo+1
        
    with open('./files/offset.txt' , 'a') as f:
        f.write('\n'.join(offset))
        f.write('\n')

    with open('./files/vocab.txt', 'a') as f:
        f.write('\n'.join(distinctWords))
        f.write('\n')
    a=offsetSize
    b=finalCount+1
    return a,b

def mergefiles(fileCount):
    flag = [0] * fileCount
    words,files,top,heap = {},{},{},[]
   
    finalCount=0
    offsetSize = 0

    data = defaultdict(list)
    i=0
    file_na = './files/index'
    while i < fileCount:
        f_name = file_na + str(i) + '.txt'
        files[i] = open(f_name, 'r')
        a=files[i].readline()
        a=a.strip()
        top[i] = a
        words[i] = top[i].split()
        x = words[i][0]
        if words[i][0] not in heap:
            heapq.heappush(heap,x)
        flag[i] = 1
        i=i+1
    count = 0
    while any(flag) == 1:
        temp = heapq.heappop(heap)
        count = count + 1
        if count%100000 == 0:
            oldFileCount = finalCount
            offsetSize,finalCount = final_write(data, finalCount, offsetSize)
            if finalCount != oldFileCount :
                data = defaultdict(list)
    
        for i in range(fileCount):
            if flag[i]==1:
                if temp == words[i][0] :
                    x=files[i].readline()
                    x=x.strip()
                    top[i] = x
                    data[temp].extend(words[i][1:])
                    if top[i]!='':
                        words[i] = top[i].split()
                        word=words[i][0]
                        if (word not in heap):
                            heapq.heappush(heap, words[i][0])
                    else:
                        files[i].close()   
                        flag[i] = 0
                                  
    offsetSize,finalCount = final_write(data, finalCount, offsetSize)

# Writing index file in outpath       
def file_handler(index, docID, outFile):
    data = []
    keyList=sorted(index.keys())
    for key in keyList:
        postings = index[key]
        string =key +' '+ ' '.join(postings)
        data.append(string)
    with open(outFile, 'w') as f:
        f.write('\n'.join(data))
        
def write_log_file(message):
    try:
        file1 = open("logfile.txt","a")
        file1.write(message)
        file1.write("\n")
        file1.close()
    except:
        print("Exception in write_log_file")

class DocHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.data=""
        self.title=""
        self.text=""
    def startElement(self,tag,attributes):
        self.data=tag
    def endElement(self,tag):
        if tag == 'page':
            docID[p_cnt] = self.title.strip().encode("ascii", errors="ignore").decode()
            title, body, info, categories, links = processText(self.text, self.title)
            Indexer( title, body, info, categories, links)
            self.data = ''
            self.title = ''
            self.text = ''
    def characters(self, content):
        if self.data == 'title':
            self.title = self.title + content
        elif self.data == 'text':
            self.text += content
        
def Parser(fileName):
    parser=xml.sax.make_parser()  #Creating XML Reader
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)  #Turning off namespaces
    Handler=DocHandler()
    parser.setContentHandler(Handler)
    parser.parse(fileName)
       
if __name__ == '__main__':
    xmlDumpDirectory=sys.argv[1]
    inverted_index_path=sys.argv[2]
    inverted_index_stat=sys.argv[3]
    tokensCount_in_dump=0
    docID = {} ## {Doc id : Title}
    PostList = defaultdict(list)
    f_cnt = 0 ### File Count
    p_cnt = 0 ### Page Count
    offset = 0 ## Offset
    directory="./files"
    os.mkdir(directory)
    # Begin Parsing
    xmlDumpList = os.listdir(xmlDumpDirectory)
    for file in xmlDumpList:
        fileName=xmlDumpDirectory+'/'+file
        parser = Parser(fileName)
    
    file_handler(PostList, docID,inverted_index_path)
    with open('./files/fileNumbers.txt', 'w') as f:
        f.write(str(p_cnt))
    offset = writeinfile(PostList, docID, f_cnt , offset)
    f_cnt = f_cnt + 1
    mergefiles(f_cnt)
    
    #Calculating tokens count in dump file and index file
    tokensCount_in_index=0
    with open(inverted_index_path,"r") as f:
        for line in f:
            tokensCount_in_index+=1
    
    with open(inverted_index_stat,"w") as f:
        f.write(str(tokensCount_in_dump))
        f.write("\n")
        f.write(str(tokensCount_in_index))
    print("Tokens Count in dump file: ", tokensCount_in_dump)    
    print("Tokens Count in Index file: ",tokensCount_in_index)
    #shutil.rmtree(directory)
     
    
#python3 indexer.py xmlDumpdirectory index.txt stat.txt    