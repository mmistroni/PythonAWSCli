'''
Created on 20 Jul 2016

@author: marco
'''
import unittest
import boto3
from pprint import pprint


class SNS_Client(object):
    
    def __init__(self):
        self.client = boto3.client('sns')
        
    def list_topics(self):
        return self.client.list_topics()['Topics']

    def list_subscriptions_by_topic(self, topicArn):
        return self.client.list_subscriptions_by_topic(TopicArn=topicArn)['Subscriptions']

    def subscribe(self, topic,protocol, endpoint):
        return self.client.subscribe()


class EC2_Client(object):
    def __init__(self):
        self.client = boto3.client('ec2')

    def start_instances(self, instanceId):
        response = self.client.start_instances(InstanceIds=[instanceId]) #'i-76bb3ed9'])
        instance_state = response['StartingInstances'][0]
        return instance_state
    
    def stop_instances(self, instanceId):
        response = self.client.stop_instances(InstanceIds=[instanceId]) #'i-76bb3ed9'])
        return response['StoppingInstances'][0]
        
    
    def list_instances(self):
        print 'Listing ec2_instances..'
        response = self.client.describe_instances()
        response_list = [dict(Id=d['Instances'][0]['InstanceId'],
                              Name=d['Instances'][0].get('Tags', [{'Value':'Unknown'}])[0]['Value'],
                              State=d['Instances'][0]['State']['Name']) for d in response['Reservations']]
        return response_list    

class Test(unittest.TestCase):


    def testName(self):
        
        client  =EC2_Client()
        params = {}
        print getattr(client, 'list_instances')(**params) #.list_instances()
        #start_instsances('i-76bb3ed9')
        #
        '''
        params = dict(instanceId='i-76bb3ed9')
        print client.stop_instances(**params)
        '''
        client = SNS_Client()
        pprint(client.list_topics())

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()