#%%
from Executor import Preprocessing as PPC
from Executor import Mageck_Converter as MG
from Executor import RPM 
from datetime import datetime
import pandas as pd
import sys

#%%
def main(Data_combination):
    Useage = ("How to use\n"+
              "Command: python UMI_Analyzer.py ProjectName Replicate ControlName TreatmentName\n"+'\n'

              "ex) python UMI_Analyzer.py 84K R1 D10 D24\n"+
              "    Project Name = 84K\n"+
              "    Replicate    = R1\n"+
              "    Control Name = D10\n"+
              '    Treatment Name = D24\n\n'
            
              'Input_File Name_Format: ProjectName_Replicate_Control or Treatment_all_random_barcode.txt\n'+
              "ex) 84K_R1_D10_all_random_barcode.txt\n\n"+
              
              'Input_NonTarget_File Name_Format: ProjectName_nontarget.txt\n'+
              'ex) 84K_nontarget.txt  or  2KABE_nontarget.txt' )
    


    if len(sys.argv) < 1:
        print(Useage)
    else:
        PPC.DataPreProcessing(Data_combination)
        RPM.TotalRPM().Return_Bread(Data_combination)
        ## MageckConverter input:
        ## SortingBarcode / RandomBarcode / D10_count / D24_Count        
        MG.RPM().MageckConverter(Data_combination)
        
    return 
#%%

Data_combination = [(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])]



#%%

if __name__ == '__main__':
    main(Data_combination)

