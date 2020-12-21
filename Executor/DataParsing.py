#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime


class Data_Parsing:
    def __init__(self):
        self.chunk = 25

    def Divide_Dictionary(self, dic, N):
        List = []
        tup = [(i,dic[i]) for i in dic]

        for i in range(N):
            begin = (len(tup)//N)*i
            end = (len(tup) // N) * (i + 1)

            if i != N-1:
                List.append(tup[begin:end])
            else:
                List.append(tup[begin:])

        DicList = []


        for k in List:
            DicList.append(dict(k))
        return DicList

    def _Barcode_Index(self,DF):
        ##반드시 Sorting_Barocde가 column 0
        t1 = datetime.now()
        n = 1
        
        SB = list(DF.iloc[:,0])
        LIST = [(SB[0],0)]
        while n < DF.shape[0]:
            if SB[n] != SB[n-1]:
                LIST.append((SB[n],n))
            else:
                pass
            n+=1
        t2 = datetime.now()

        print('Barcode_Indexing:', t2-t1)

        return LIST

    def Divide_PDS_data(self,DF):
        BClist = self._Barcode_Index(DF)

        dflist = []

        n = 0 
        t1 = datetime.now()
        while n < len(BClist):
            idx = BClist[n][1] 
            
            if n == len(BClist)-1:
                if DF[idx:].shape[0] <6:
                    pass
                else:
                    dflist.append(DF[idx:])
            else:
                if DF[idx:BClist[n+1][1]].shape[0] <6:
                    pass
                else:
                    dflist.append(DF[idx:BClist[n+1][1]])
            n+=1
        t2 = datetime.now()

        print('Make DF list:', t2-t1)
        return dflist

# %%
