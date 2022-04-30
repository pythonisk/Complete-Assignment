import pandas as pd, os
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
from datetime import datetime


dirname = 'C:/Users/inumu/OneDrive/Desktop/docs/sravanth/S22 5110 Assignment 4/Multiprocessing'
filedir = os.listdir("{}".format(dirname))
file_names = ["{}/".format(dirname)+filename for filename in filedir if filename.endswith('csv')]
file_names = file_names[:50] # Optional, Uncomment to proceed to limitted file names; Eg: 50 files




def apply_operation(df_chunks):
    first_3_count = pd.DataFrame([[df_chunks.__len__(), df_chunks.customerID.unique().__len__(), df_chunks.PaymentMethod.unique().__len__()]], columns=['Total rows accross all files', 'Customer Id Count(Unique)', 'Payment Method count(Unique)'])
    return first_3_count

def apply_perfile_operation(perfile_name):
    per_filelist = list()
    # print('Perfilename: --->', perfile_name)
    for filename in perfile_name:
        chunk_dataset = pd.read_csv(filename)
        per_filelist.append([filename.rsplit('/', 1)[1], chunk_dataset[chunk_dataset.Churn == 'Yes']['customerID'].unique().__len__(), chunk_dataset[chunk_dataset.PaperlessBilling == 'Yes']['customerID'].unique().__len__()])

    return per_filelist



def MultiReadingProcess(prs):
    start_time = datetime.now()
    processes = prs
    # print('--->', file_names.__len__())
    chunk_size = int(file_names.__len__()/processes)

    chunks_lst = list()
    num_chunks = len(file_names) // chunk_size + 1

    # for i in range(num_chunks-1):
    for i in tqdm(range(num_chunks-1)):
        concat_data = pd.concat(map(pd.read_csv, file_names[i*chunk_size:(i+1)*chunk_size]))
        chunks_lst.append(concat_data)

    with ThreadPool(processes) as p:
        result = p.map(apply_operation, chunks_lst)

    df_reconstructed = pd.concat(result)
    multi_dataset = pd.DataFrame([[df_reconstructed['Total rows accross all files'].sum(), df_reconstructed['Customer Id Count(Unique)'].sum(), df_reconstructed['Payment Method count(Unique)'].sum()]], columns=['Total rows accross all files', 'Customer Id Count(Unique)', 'Payment Method count(Unique)'])

    pd.DataFrame(multi_dataset['Total rows accross all files'].tolist(), columns=['Total rows accross all files']).to_csv('Total Records Count.csv', index=False)
    pd.DataFrame(multi_dataset['Payment Method count(Unique)'].tolist(), columns=['Payment Method count(Unique)']).to_csv('Total customers have Payment Method unique.csv', index=False)
    pd.DataFrame(multi_dataset['Customer Id Count(Unique)'].tolist(), columns=['Customer Id Count(Unique)']).to_csv('Total customers having CustomerID unique.csv', index=False)



    # print(multi_dataset)
    # multi_dataset.to_csv('Multiprocess_count_result_30_04_2022.csv', index=False)
    print('Total time consumption: ', datetime.now() - start_time)
    
    return 'MultiReadingPRocess Done!'


def Perfile_process(prs):
    start_time = datetime.now()
    processes_count = prs
    chunks_lst = list()
    chunk_size = int(file_names.__len__()/processes_count)
    num_chunks = len(file_names) // chunk_size + 1

    for i in range(num_chunks-1):
        concat_data = file_names[i*chunk_size:(i+1)*chunk_size]
        chunks_lst.append(concat_data)
    
    with ThreadPool(processes_count) as p:
        # result_list = p.map(apply_perfile_operation, chunks_lst)
        result_list = tqdm(p.map(apply_perfile_operation, chunks_lst))

    # print(result_list, '\n')
    final_list = list()
    for sub_list in result_list:
        final_list += sub_list
    


    Perfile_dataset = pd.DataFrame(final_list, columns = ['File Name', 'Per file, number of customers that have churned', 'Per file, number of customers that have paperless billing'])
    # print(Perfile_dataset)
    # print(pd.DataFrame(list(zip(Perfile_dataset['File Name'].tolist(), Perfile_dataset['Per file, number of customers that have churned'].tolist())), columns=['File Name', 'Per file, number of customers that have churned']))
    pd.DataFrame(list(zip(Perfile_dataset['File Name'].tolist(), Perfile_dataset['Per file, number of customers that have churned'].tolist())), columns=['File Name', 'Per file, number of customers that have churned']).to_csv('Per file data for Churned.csv', index=False)
    # print(pd.DataFrame(list(zip(Perfile_dataset['File Name'].tolist(), Perfile_dataset['Per file, number of customers that have paperless billing'].tolist())), columns=['File Name', 'Per file, number of customers that have paperless billing']))
    pd.DataFrame(pd.DataFrame(list(zip(Perfile_dataset['File Name'].tolist(), Perfile_dataset['Per file, number of customers that have paperless billing'].tolist())), columns=['File Name', 'Per file, number of customers that have paperless billing'])).to_csv('Per file data for PaperlessBilling.csv', index=False)
    

    # Perfile_dataset.to_csv('Per file, Churn_and_PaperlessBilling_Count_30_04_2022.csv', index=False)
    print('Total time consumption: ', datetime.now() - start_time)

    return 'Perfile process Completed!'



MultiReadingProcess(1)

# Perfile_process(1)


    
