#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime



# %%

class clsDirectional:

    def _Make_Every_ED1_Seq(self,Rnd_Barcode):
        Base = ['A', 'T', 'G', 'C']
        ED1_list = []
        for i in range(len(Rnd_Barcode)):
            if i == 0:
                for N in Base:
                    ED1 = N + Rnd_Barcode[1:]
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)
            elif i == len(Rnd_Barcode) - 1:
                for N in Base:
                    ED1 = Rnd_Barcode[:-1] + N
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)
            else:
                for N in Base:
                    ED1 = Rnd_Barcode[:i] + N + Rnd_Barcode[i + 1:]
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)

        return ED1_list

    def _Possible_ED1(self, df):
        rndList = list(df.iloc[:,1])
        rnd_dic = dict(zip(df.iloc[:,1],df.iloc[:,2]))

        tupList = []
        final = []
        for rnd in rndList:
            if type(rnd) == float:
                print(df)
                pass
            else:
                possibleEd1 = clsDirectional()._Make_Every_ED1_Seq(rnd)
                trueEd1     = [i for i in possibleEd1 if i in rndList]
                if len(trueEd1) == 0:
                    final.append((rnd,rnd_dic[rnd]))
                else:
                    for k in trueEd1:
                        tupList.append([(rnd,rnd_dic[rnd]),(k,rnd_dic[k])])
        
        ## Final: Rnd without ED1 in the total Rnd List
        ## tupList: Rnd tup list with ED1 in the total Rnd List
        return tupList, final

    def Compare_Count(self,A, B):

        if A >= 3 * B - 1:
            return 1
        else:
            return 0

    def _Same_Count(self, Ed1tupList):
        LIST = []
        for tup in Ed1tupList:
            if tup[0][1] == tup[1][1]:
                LIST.extend(tup)
            else:
                pass
        LIST = list(set(LIST))
        return LIST

    def _ED1_Group(self, possibleED1):
        ## possibleED1 = return of _Possible_ED1
        ED1tupList = possibleED1[0]
        final = possibleED1[1] 

        trueEd1 = [ tup for tup in ED1tupList if clsDirectional().Compare_Count(tup[0][1],tup[1][1]) == 1]
        same = self._Same_Count(ED1tupList)
        
        dic = {}

        for tup in trueEd1:
            if tup[0] not in dic:
                dic[tup[0]] = [tup[1]]
            else:
                dic[tup[0]].append(tup[1])

        overlap = []
        for t in dic:
            for k in dic[t]:        
                if k not in same:
                    pass
                else:
                    overlap.append(k)

        truesame = [i for i in same if i not in overlap]

        for tup in truesame:
             final.append(tup)

        
        for tup in dic:
            read = tup[1]+sum([i[1] for i in dic[tup]])
            final.append((tup[0],read))

        df = pd.DataFrame(final)
        return df


    def _Directional_Adjacency(self, eachDF):
        SB = eachDF.iloc[0,0]
        possibleED1 = self._Possible_ED1(eachDF)
        finalRnD   = self._ED1_Group(possibleED1)
        
        finalRnD['Sorting_barcode'] = SB
        finalRnD = finalRnD[['Sorting_barcode',0,1]]
        return finalRnD


    def Multi_Directional_Adjacency(self, Splice_PDS_list):
        t1 = datetime.now()
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []

            for eachDF in Splice_PDS_list:
                fut = executor.submit(self._Directional_Adjacency, eachDF)
                futs.append(fut)

            merged = []
            for fut in futs:
                merged.append(fut.result())
            final_pds = pd.concat(merged)
            t2 = datetime.now()
            print('Directional_Adjacency: ', t2-t1)
            return final_pds


# %%
