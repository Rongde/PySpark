# ** /
# @ Author: Rongde
# @ Description: https://flextravelsolutions.atlassian.net/browse/CS-441
# @ Date:
# @ Param:
# @ return:
# ** /
import datetime
import re
import pandas as pd
import time
import configparser

# 'local' : local dev, 'remote' : glue dep
def connection(type):
    if type == 'local':
        print('local')


    elif type == 'remote':
        print('remote')

    else:
        raise Exception('Connection Error Please Check Parameter \'Type\'')


def insert_ourlist(row, data):
    data = pd.concat([data,pd.DataFrame.from_dict({
                                         'code': [row['supplier_code']],
                                         'name': [row['supplier_name']],
                                         'type':['INSERT']
                                         })])
    return data


def update_ourlist(row, data):
    data = pd.concat([data, pd.DataFrame.from_dict({
                                          'code': [row['our_code']],
                                          'name': [row['our_name']],
                                          'type': ['UPDATE']
                                          })])
    return data

def comparison(supplier_list, our_list):

    # 1.var
    # row of supplier list and our list
    iRow_supplier_list = supplier_list.shape[0]
    iRow_our_list = our_list.shape[0]
    # check_index_list
    check_index_list = list()
    # result data
    data_result = pd.DataFrame(columns=['code','name','type'])

    # 2.check supplier list
    for i in range(iRow_supplier_list):
        try:
            row = our_list[our_list['our_code'] == int(re.match(r'\d+', supplier_list.loc[i]['supplier_code']).group())]
        except Exception as e:
            print(e)
        if (row.empty):
                data_result = insert_ourlist(row = supplier_list.loc[i], data = data_result)
        else:
            check_index_list.append(row.index[0])

    data_result = data_result.reset_index(drop=True)
    check_index_list = sorted(check_index_list)

    # 3.check our list
    list_dif = list(set(list(range(0,iRow_our_list))).difference(set(check_index_list)))

    for i in list_dif:
        data_result = update_ourlist(row = our_list.loc[i], data = data_result)


    data_result = data_result.reset_index(drop=True)

    return data_result

if __name__ == '__main__':

    connection('abc')

    #
    # # read parameters
    # config = configparser.ConfigParser()
    # config.read('config.ini')
    # path_supplier_list = config['file_path']['path_supplier_list']
    # path_our_list = config['file_path']['path_our_list']
    # path_result = config['file_path']['path_result']
    #
    # print(config.sections())
    #
    # # colunms name
    # col_our_code = 'our_code'
    # col_our_name = 'our_name'
    # col_supplier_code = 'supplier_code'
    # col_supplier_name = 'supplier_name'
    #
    # # 2.import data
    # our_list = pd.read_csv(path_our_list)
    # supplier_list = pd.read_csv(path_supplier_list)
    #
    # # 3. Comparison
    # data = comparison(supplier_list = supplier_list, our_list = our_list)
    # data.to_csv(path_result + 'result_' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.csv')
    #
    # # 4.compute running time
    # endTime = time.time()
    # runTime = startTime - endTime
    # print('Running time：' + str(endTime - startTime))


