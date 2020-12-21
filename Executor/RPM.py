#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from Executor import DataParsing as DP

#%%
class TotalRPM:

    def Read_PDS(self, Data_combination):
        t1 = datetime.now()
        PDS_data = pd.read_csv( ##{}_{}_Raw_Data_Integration_File
            'Result/{}_{}_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1])
            , delimiter='\t')
        t2 = datetime.now()
    
        print('Read_PDS', t2 - t1)
        return PDS_data

    def Read_PDS_removeD24(self, Data_combination):
        t1 = datetime.now()
        PDS_data = pd.read_csv( ##{}_{}_Raw_Data_Integration_File
            'Result/{}_{}_Remove_null_D24_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1])
            , delimiter='\t')
        t2 = datetime.now()

        print('Read_PDS', t2 - t1)
        return PDS_data



    def RPM(self,DF):

        tot10 = DF['D10_Count'].sum()
        tot24 = DF['D24_Count'].sum()

        rpm10 = lambda x:x/tot10*1e6
        rpm24 = lambda x:x/tot24*1e6

        DF['D10_RPM'] = DF['D10_Count'].apply(rpm10)
        DF['D24_RPM'] = DF['D24_Count'].apply(rpm24)

        DF['Fold_Change'] = DF['D24_RPM']/DF['D10_RPM']


        return DF




    def Write_Pandas(self, rpmdf, removeD24_rpm_df, Data_combination):

        rpmdf.to_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(Data_combination[0][0], Data_combination[0][1]),
                  sep='\t', index=False, header=['Sorting_Barcode', 'RandomBarcode', 'D10_Count', 'D24_Count'
                                                 ,'D10_RPM','D24_RPM', 'Fold_Change'])
                                                 
        removeD24_rpm_df.to_csv('Result/{}_{}_Remove_D24_totalRPM_FC_Result.txt'.format(Data_combination[0][0], Data_combination[0][1]),
                  sep='\t', index=False, header=['Sorting_Barcode', 'RandomBarcode', 'D10_Count', 'D24_Count'
                                                 ,'D10_RPM','D24_RPM', 'Fold_Change'])
        
        return 



    def Return_Bread(self,Data_combination):

        a = self.Read_PDS(Data_combination) 
        b = self.Read_PDS_removeD24(Data_combination)       

        t3 = datetime.now()
        c = self.RPM(a)
        d = self.RPM(b)


        t4 = datetime.now()
        print('RPM_calculation: ', t4-t3)
        self.Write_Pandas(c,d,Data_combination)
        
        return 

# %%
