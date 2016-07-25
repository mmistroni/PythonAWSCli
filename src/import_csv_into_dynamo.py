'''
Created on 25 Jul 2016

@author: marco
'''
import unittest
from pprint import pprint

import boto3

MY_ACCESS_KEY_ID = 'copy your access key ID here'
MY_SECRET_ACCESS_KEY = 'copy your secrete access key here'


def do_batch_write(items, table_name, dynamodb_table, dynamodb_conn):
    '''
    From https://gist.github.com/griggheo/2698152#file-gistfile1-py-L31
    '''
    with dynamodb_table.batch_writer() as batch:
        for item in items:
            pprint(item)
            batch.put_item(Item=item)
        
    '''
    batch_list = dynamodb_conn.new_batch_write_list()
    batch_list.add_batch(dynamodb_table, puts=items)
    while True:
        response = dynamodb_conn.batch_write_item(batch_list)
        unprocessed = response.get('UnprocessedItems', None)
        if not unprocessed:
            break
        batch_list = dynamodb_conn.new_batch_write_list()
        unprocessed_list = unprocessed[table_name]
        items = []
        for u in unprocessed_list:
            item_attr = u['PutRequest']['Item']
            item = dynamodb_table.put_item(
                    Item=item_attr
            )
            items.append(item)
        batch_list.add_batch(dynamodb_table, puts=items)
    '''
       
def import_csv_to_dynamodb(table_name, csv_file_name, colunm_names, column_types):
    '''
    Import a CSV file to a DynamoDB table
    '''        
    dynamodb_conn = boto3.resource('dynamodb')
    dynamodb_table = dynamodb_conn.Table('shares')     
    BATCH_COUNT = 2 # 25 is the maximum batch size for Amazon DynamoDB

    items = []

    count = 0
    csv_file = open(csv_file_name, 'r')
    for cur_line in csv_file:
        count += 1
        cur_line = cur_line.strip().split(',')
        print cur_line, len(cur_line)

        row = {}
        for colunm_number, colunm_name in enumerate(colunm_names):
            row[colunm_name] = column_types[colunm_number](cur_line[colunm_number])
        print 'out of here'
        item = dynamodb_table.put_item(
                    Item=row
            )           
        items.append(row)

        if count % BATCH_COUNT == 0:
            print 'batch write start ... ', 
            do_batch_write(items, table_name, dynamodb_table, dynamodb_conn)
            items = []
            print 'batch done! (row number: ' + str(count) + ')'

    # flush remaining items, if any
    if len(items) > 0: 
        do_batch_write(items, table_name, dynamodb_table, dynamodb_conn)


    csv_file.close() 


def main():
    '''
    Demonstration of the use of import_csv_to_dynamodb()
    We assume the existence of a table named `test_persons`, with
    - Last_name as primary hash key (type: string)
    - First_name as primary range key (type: string)
    '''
    colunm_names = ['ticker', 'asofdate', 'latest','currentEps','forwardEps','movingAvg', 'exDiv', 'peg']
    
    table_name = 'shares'
    csv_file_name = 'shares.csv'
    column_types = [str, str, str, str, str, str, str, str]
    import_csv_to_dynamodb(table_name, csv_file_name, colunm_names, column_types)


    
class Test(unittest.TestCase):


    def testName(self):
        main()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()