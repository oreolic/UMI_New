#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from Executor import Directional as DA
from Executor import DataParsing as DP
from datetime import datetime
import statistics
import pathlib
import os
import operator
import sys



#%%

class clsInput:

    def _Input_Data_Parsing(self):
        FileList = [sys.argv[i] for i in range(1, len(sys.argv))]

        ParsedData = []
        for eachFile in FileList:
            parsed = self._SplitingFileName(eachFile)
            ParsedData.append(parsed)

        return ParsedData



    def _SplitingFileName(self, FileName):
        FileInfo = FileName[:-23]
        Proj,Rep,Day = FileInfo.split('_')
        return Proj,Rep,Day



    def DataCombination(self):
        ParsedList = self._Input_Data_Parsing()

        DataComb = []
        n = 0
        while n < len(ParsedList):
            DataComb.append((ParsedList[n],ParsedList[n+1]))
            n += 2

        return DataComb


#%%
class PreProcessing_of_Raw_Data:
    """
 
    """

    def __init__(self, data_type, replicate, day):
        self.data_type = data_type
        self.replicate = replicate
        self.day = day
        self.chunk = 25

        return

    def Read_RAW_data(self):
        t1 = datetime.now()  ##{}_{}{}_all_random_barcode.txt 24K_R1_D10_all_random_barcode
        if self.data_type =='84K':
            pds_data = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t',index_col=0)
            
        elif self.data_type =='24K':
             pds_data = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t',index_col=0)
                      
        else:
            pds_data = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t',index_col=0)            

        pds_data = pds_data.reset_index()
        if pds_data.shape[1] == 5:
            pds_data.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            PDS = pds_data[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode',
                    ' Total_reads_of_SB']]

            PDS = PDS.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return PDS
        
        elif pds_data.shape[1] == 3:
            pds_data.columns = ['Sorting_barcode', 'RandomBarcode', '{}_Count'.format(self.day)]
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)           
            
            return pds_data

        elif pds_data.shape[1] == 4:
            pds_data.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            PDS = pds_data[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode']]
            
            PDS = PDS.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return PDS



    def DFlist (self, PDS):
        DFlist = DP.Data_Parsing().Divide_PDS_data(PDS)
        return DFlist


    
    
#%%
class clsCombine_Data:

    def _MultiConverDic(self, DFlist):
        with ProcessPoolExecutor(max_workers = 25) as executor:
            futs = []
            for i in range(25):
                begin_idx = (len(DFlist) // 25) * i
                end_idx = (len(DFlist) // 25) * (i + 1)

                if i != 25 - 1:
                    fut = executor.submit(self._ConvertTup, DFlist[begin_idx:end_idx])
                    futs.append(fut)
                else:
                    fut = executor.submit(self._ConvertTup, DFlist[begin_idx:])
                    futs.append(fut)

            merged = []
            for fut in futs:
                merged += fut.result()            
            dic = dict(merged)   

            return dic

    def _DFconcate(self,DF1,DF2):
        
        df2dic = dict(zip(DF2.iloc[:,1],DF2.iloc[:,2]))
        
        LIST = []

        for idx in range(DF1.shape[0]):
            sb = DF1.iloc[idx,0]
            rndbcd = DF1.iloc[idx,1]
            D10 = DF1.iloc[idx,2]

            if rndbcd not in df2dic:
                eachlist = [sb,rndbcd,D10,0]
                LIST.append(eachlist)
            else:                
                eachlist = [sb,rndbcd,D10,df2dic[rndbcd]]
                LIST.append(eachlist)

        df = pd.DataFrame(LIST,columns=['Sorting_barcode','RandomBarcode','D10_Count','D24_Count'])           
        
        return df 

                
    def _ConvertTup(self, DFlist):
        Tuplist = []
        for eachDF in DFlist:
            SB = eachDF.iloc[0,0]
            Tuplist.append((SB,eachDF))
        return Tuplist


   

    def MultiCombineData(self,PreProcData):
        t1 = datetime.now()
        DF1 = PreProcData[0]
        DF2 = PreProcData[1]
        
        DFlist1 = DP.Data_Parsing().Divide_PDS_data(DF1)
        DFlist2 = DP.Data_Parsing().Divide_PDS_data(DF2)        

        dic1 = {} ## Day 10 

        for eachdf in DFlist1:
            sb = eachdf.iloc[0,0]
            dic1[sb] = eachdf

        dic2 = {} ## Day 24

        for eachdf2 in DFlist2:
            sb2 = eachdf2.iloc[0,0]
            dic2[sb2] = eachdf2

        final = {}
       
        for sb in dic1:
            if sb not in dic2:                
                eachdf = dic1[sb]
                eachdf.loc[:,'D24_Count'] = 0 
                final[sb] = eachdf
            else:
                eachdf1 = dic1[sb]
                eachdf2 = dic2[sb]

                combdf = clsCombine_Data()._DFconcate(eachdf1, eachdf2)
                final[sb] = combdf 
        
        t2 = datetime.now()
        print('Combine_Data: ', t2-t1)

        return final


        





        
       

#%%

class clsRemove5:  


    def _Remove5(self, eachDF):
        final = []
        if eachDF.shape[0] < 6:
            ## The Number of clones under 6 are eliminated
            return 

        else: 
            for idx in eachDF.index:
                D10 = eachDF.loc[idx,'D10_Count']
                D24 = eachDF.loc[idx,'D24_Count']

                if D10+D24 <11 :
                    pass
                else:
                    final.append(eachDF.loc[idx])
        
        if len(final) == 0:
            return
        else:
            df = pd.concat(final,axis=1).transpose()

            return df



    def Multi_Remove5(self,Combined_DFdic):
        DFList = [Combined_DFdic[i] for i in Combined_DFdic]
 
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []
            for eachDF in DFList:
                fut = executor.submit(self._Remove5,eachDF)
                futs.append(fut)
            
            merged = []
            for fut in futs:
                merged.append(fut.result())
            
            DF = pd.concat(merged)

            return DF



    def _Remove_nullD24(self,eachDF):
        final = []
        if eachDF.shape[0] < 6:
            ## The Number of clones under 6 are eliminated
            return 

        else: 
            for idx in eachDF.index:
                D10 = eachDF.loc[idx,'D10_Count']
                D24 = eachDF.loc[idx,'D24_Count']

                if D10+D24 <11:
                    pass
                else:
                    if D24 == 0:
                        if D10 <11:
                            pass
                        else:
                            final.append(eachDF.loc[idx])
                    else:
                        final.append(eachDF.loc[idx])
                        
        if len(final) == 0:
            return
        else:
            df = pd.concat(final,axis=1).transpose()
            return df



    def Multi_Remove_nullD24(self,Combined_DFdic):
        DFList = [Combined_DFdic[i] for i in Combined_DFdic]
    
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []
            for eachDF in DFList:
                fut = executor.submit(self._Remove_nullD24,eachDF)
                futs.append(fut)
            
            merged = []
            for fut in futs:
                merged.append(fut.result())
            
            DF = pd.concat(merged)

            return DF


#%%
def _Directional_Adjacency(DFlist):
    D = DA.clsDirectional()
    DA_DFlist = D.Multi_Directional_Adjacency(DFlist)
    return DA_DFlist


def _PreProcessingRaw_data(Data_combination):

    for tup in Data_combination:
        Cont = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[2])
        Trea = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[3])

        D10_PDS = Cont.Read_RAW_data()
        D24_PDS = Trea.Read_RAW_data()

        D10_DFlist = Cont.DFlist(D10_PDS)   
        D24_DFlist = Trea.DFlist(D24_PDS)

        print('Start Calculate Directional Adjacency')
        DA_D10_DFlist = _Directional_Adjacency(D10_DFlist).rename(columns={0:'RandomBarcode',1:'D10_Count'})
        DA_D24_DFlist = _Directional_Adjacency(D24_DFlist).rename(columns={0:'RandomBarcode',1:'D24_Count'})

        
        DA_D10_DFlist.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[2]), sep = '\t'
                             ,index=False)
        DA_D24_DFlist.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[3]), sep = '\t'
                             ,index=False)
        
        
    ## Output: (D10_DFlist,D24_DFlist)
        return DA_D10_DFlist,DA_D24_DFlist





def _Combine_D10_D24(PreProcessing_DATA):
    ## Input:  (D10_DFlist,D24_DFlist)
    CB = clsCombine_Data()
    DFdic = CB.MultiCombineData(PreProcessing_DATA)        
    return DFdic


def _Remove5(DFdic,Data_combination):
    R5 = clsRemove5()
    DF_R5 = R5.Multi_Remove5(DFdic)
    return DF_R5

def _Remove_null_D24(DFdic,Data_combination):
    R5 = clsRemove5()
    DF_R5 = R5.Multi_Remove_nullD24(DFdic)
    return DF_R5



#%%

def DataPreProcessing(Data_combination):  
    PreProcData = _PreProcessingRaw_data(Data_combination)
    ## 
    print('Start Combine s/p Directional Result')    
    DFdic = _Combine_D10_D24(PreProcData)    
    DF_R5 = _Remove5(DFdic, Data_combination)  
       
    DF_R5.to_csv('Result/{}_{}_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1]),sep='\t',index=None)
    
    
    DF_woD24 = _Remove_null_D24(DFdic, Data_combination)
    DF_woD24.to_csv('Result/{}_{}_Remove_null_D24_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1]),sep='\t',index=None)
    
    
    return

